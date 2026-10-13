import logging
from dataclasses import dataclass, field

import pandas as pd
from pandas import DataFrame

from include.aws.redshift.client import RedshiftClient
from include.aws.redshift.connection import RedshiftConnection
from include.config import TIMEZONE
from include.etl.contract import apply_contract, select_version
from include.etl.file_types import Parquet, build_file_type
from include.etl.layer import Layer, LayerUtils, files_to_records, records_to_files
from include.etl.silver_to_gold import arrow_schema, to_gold

logger = logging.getLogger(__name__)

MAX_LOGGED_FILES = 20


def log_group(title: str, lines: list[str]) -> None:
    """Loga `lines` entre `::group::`/`::endgroup::`, que a UI do Airflow
    mostra como uma seção recolhível. Limita a `MAX_LOGGED_FILES` linhas."""
    logger.info(f"::group::{title}")
    for line in lines[:MAX_LOGGED_FILES]:
        logger.info(line)
    if len(lines) > MAX_LOGGED_FILES:
        logger.info(f"... e mais {len(lines) - MAX_LOGGED_FILES}")
    logger.info("::endgroup::")


def log_files(
    layer_name: str, bucket: str, prefixes: list[str], files: DataFrame
) -> None:
    """Loga onde a camada foi pesquisada e os arquivos encontrados."""
    label = layer_name.capitalize()
    for prefix in prefixes:
        logger.info(f"{label}: procurando em {bucket}/{prefix}")

    if files.empty:
        logger.warning(f"{label}: nenhum arquivo encontrado.")
        return

    logger.info(f"{label}: {len(files)} arquivo(s) encontrado(s).")
    log_group(
        f"Arquivos na {layer_name} ({len(files)})",
        [
            f"{row.FileName}  modificado em {row.LastModified:%Y-%m-%d %H:%M} UTC  "
            f"{row.Size / 1024:,.0f} KB"
            for row in files.itertuples()
        ],
    )


def compare_layers(
    source_name: str, source: list[dict], target_name: str, target: list[dict]
) -> list[dict]:
    """Arquivos de `source` novos ou modificados em relação a `target`,
    no formato do XCom, com o `LastModified` do `source`."""
    pending = LayerUtils().compare(records_to_files(source), records_to_files(target))
    is_new = pending["LastModified_y"].isna()
    label = f"{source_name.capitalize()} x {target_name}"

    if pending.empty:
        logger.info(f"{label}: nenhum arquivo novo ou modificado.")
    else:
        logger.info(
            f"{label}: {len(pending)} arquivo(s) para processar "
            f"({is_new.sum()} novo(s), {(~is_new).sum()} modificado(s))."
        )
        log_group(
            f"Arquivos para processar ({len(pending)})",
            [
                f"{row.FileName}  "
                + (
                    "novo"
                    if new
                    else f"modificado ({target_name} de {row.LastModified_y:%Y-%m-%d %H:%M} UTC)"
                )
                for row, new in zip(pending.itertuples(), is_new)
            ],
        )

    return files_to_records(pending.rename(columns={"LastModified_x": "LastModified"}))


@dataclass
class MedallionPipeline:
    path: str
    file: str
    bronze_to_silver: dict | None = None
    silver_to_gold: dict | None = None
    redshift: dict | None = None
    layer: Layer = field(init=False)

    def __post_init__(self) -> None:
        self.layer = Layer(self.path, self.file)

    @classmethod
    def from_contract(cls, contract: dict) -> "MedallionPipeline":
        return cls(
            path=contract["s3"]["path"],
            file=contract["s3"]["file"],
            bronze_to_silver=contract.get("bronze_to_silver"),
            silver_to_gold=contract.get("silver_to_gold"),
            redshift=contract.get("redshift"),
        )

    def list_bronze_files(self) -> list[dict]:
        files = self.layer.get_bronze_files()
        log_files(
            "bronze",
            self.layer.settings.bucket_bronze,
            self.layer.file_prefixes(),
            files,
        )
        return files_to_records(files)

    def list_silver_files(self) -> list[dict]:
        files = self.layer.get_silver_files()
        log_files(
            "silver",
            self.layer.settings.bucket_silver,
            self.layer.file_prefixes(),
            files,
        )
        return files_to_records(files)

    def compare_bronze_and_silver(
        self, bronze: list[dict], silver: list[dict]
    ) -> list[dict]:
        """Arquivos da bronze sem correspondente na silver (novos) ou
        modificados depois da versão que está na silver."""
        return compare_layers("bronze", bronze, "silver", silver)

    def compare_silver_and_gold(
        self, silver: list[dict], gold: list[dict]
    ) -> list[dict]:
        """Arquivos da silver sem correspondente na gold (novos) ou
        modificados depois da versão que está na gold."""
        return compare_layers("silver", silver, "gold", gold)

    def transform_bronze_to_silver(self, record: dict) -> dict:
        """Lê um arquivo da bronze, aplica o contrato e grava na silver."""

        if not self.bronze_to_silver:
            raise ValueError("O YAML do pipeline não tem o bloco `bronze_to_silver`.")

        settings = self.layer.settings
        s3 = self.layer.s3_client
        file_name, bronze_key = record["FileName"], record["Key"]

        version = select_version(self.bronze_to_silver["versions"], file_name)
        logger.info(
            f"{file_name}: contrato a partir de {version['from']} "
            f"({len(version['columns'])} colunas, origem {version['origin']['type']})"
        )

        reader = build_file_type(version["origin"], s3, None)
        bronze = reader.get(settings.bucket_bronze, bronze_key)
        silver = apply_contract(bronze, version, version.get("footer_column"))

        writer = build_file_type(self.bronze_to_silver["destination"], s3, None)
        prefix = bronze_key.rsplit("/", 1)[0]
        writer.put(silver, settings.bucket_silver, prefix, file_name)
        silver_key = f"{prefix}/{file_name}.{writer.extension}"

        logger.info(
            f"{file_name}: {len(bronze)} linha(s) lida(s) da bronze, "
            f"{len(silver)} gravada(s) em {settings.bucket_silver}/{silver_key}"
        )
        return {"FileName": file_name, "Key": silver_key, "rows": len(silver)}

    def list_gold_files(self) -> list[dict]:
        files = self.layer.get_gold_files()
        log_files(
            "gold",
            self.layer.settings.bucket_gold,
            self.layer.gold_file_prefixes(),
            files,
        )
        return files_to_records(files)

    def transform_silver_to_gold(self, record: dict) -> dict:
        """Lê um arquivo da silver, aplica o schema da gold e grava em Parquet
        (`<env>/<path>/<arquivo>.parquet`, sem partição por ano)."""
        if not self.silver_to_gold:
            raise ValueError("O YAML do pipeline não tem o bloco `silver_to_gold`.")

        settings = self.layer.settings
        s3 = self.layer.s3_client
        file_name, silver_key = record["FileName"], record["Key"]
        columns = self.silver_to_gold["schema"]

        reader = build_file_type(self.silver_to_gold["origin"], s3, None)
        silver = reader.get(settings.bucket_silver, silver_key)
        gold = to_gold(silver, columns, file_name)

        prefix = self.layer.gold_prefix()
        Parquet(s3, schema=arrow_schema(columns)).put(
            gold, settings.bucket_gold, prefix, file_name
        )
        gold_key = f"{prefix}/{file_name}.parquet"

        logger.info(
            f"{file_name}: {len(silver)} linha(s) lida(s) da silver, "
            f"{len(gold)} gravada(s) em {settings.bucket_gold}/{gold_key} "
            f"(dt_ref {gold['dt_ref'].iloc[0] if len(gold) else '-'})"
        )
        return {"FileName": file_name, "Key": gold_key, "rows": len(gold)}

    def list_redshift_files(self) -> list[dict]:
        """`dt_ref` já carregados na tabela de destino e quando foram carregados
        pela última vez (`MAX(dt_relatorio)`). Cada `dt_ref` corresponde a um
        arquivo da gold."""
        table = f"{self.redshift['schema']}.{self.redshift['table']}"
        client = RedshiftClient(RedshiftConnection(database=self.redshift["database"]))
        loaded = client.get(
            f"SELECT dt_ref, MAX(dt_relatorio) AS dt_relatorio FROM {table} GROUP BY dt_ref"
        )

        if loaded.empty:
            logger.warning(f"Redshift: {table} sem nenhum dt_ref carregado.")
        else:
            logger.info(
                f"Redshift: {len(loaded)} dt_ref carregado(s) em "
                f"{self.redshift['database']}.{table} "
                f"(de {loaded['dt_ref'].min()} a {loaded['dt_ref'].max()})."
            )
        return [
            {
                "dt_ref": str(row.dt_ref),
                "dt_relatorio": pd.Timestamp(row.dt_relatorio).isoformat(),
            }
            for row in loaded.itertuples()
        ]

    def compare_gold_and_redshift(
        self, gold: list[dict], redshift: list[dict]
    ) -> list[dict]:
        """Arquivos da gold a carregar: `insert` (dt_ref ainda não está na
        tabela) ou `update` (arquivo modificado depois da última carga)."""
        loaded = pd.DataFrame(redshift, columns=["dt_ref", "dt_relatorio"])
        loaded["dt_relatorio"] = pd.to_datetime(loaded["dt_relatorio"])
        # O LayerUtils compara com o LastModified da gold em horário de Brasília
        # sem fuso; se a coluna vier como timestamptz, normaliza igual.
        if loaded["dt_relatorio"].dt.tz is not None:
            loaded["dt_relatorio"] = (
                loaded["dt_relatorio"].dt.tz_convert(TIMEZONE).dt.tz_localize(None)
            )

        pending = LayerUtils().compare_with_redshift(records_to_files(gold), loaded)
        pending = pending.assign(
            FileName=pending["Key"].str.split("/").str[-1].str.split(".").str[0]
        )

        if pending.empty:
            logger.info("Gold x redshift: nenhum arquivo novo ou modificado.")
        else:
            counts = pending["tipo"].value_counts()
            logger.info(
                f"Gold x redshift: {len(pending)} arquivo(s) para carregar "
                f"({counts.get('insert', 0)} insert, {counts.get('update', 0)} update)."
            )
            log_group(
                f"Arquivos para carregar ({len(pending)})",
                [f"{row.FileName}  {row.tipo}" for row in pending.itertuples()],
            )
        return pending[["Key", "FileName", "tipo"]].to_dict("records")
