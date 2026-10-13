import logging
from dataclasses import dataclass, field

from pandas import DataFrame

from include.etl.contract import apply_contract, select_version
from include.etl.file_types import build_file_type
from include.etl.layer import Layer, LayerUtils, files_to_records, records_to_files

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


@dataclass
class MedallionPipeline:
    path: str
    file: str
    bronze_to_silver: dict | None = None
    layer: Layer = field(init=False)

    def __post_init__(self) -> None:
        self.layer = Layer(self.path, self.file)

    @classmethod
    def from_contract(cls, contract: dict) -> "MedallionPipeline":
        return cls(
            path=contract["s3"]["path"],
            file=contract["s3"]["file"],
            bronze_to_silver=contract.get("bronze_to_silver"),
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
        pending = LayerUtils().compare(
            records_to_files(bronze), records_to_files(silver)
        )
        is_new = pending["LastModified_y"].isna()

        if pending.empty:
            logger.info("Bronze x silver: nenhum arquivo novo ou modificado.")
        else:
            logger.info(
                f"Bronze x silver: {len(pending)} arquivo(s) para processar "
                f"({is_new.sum()} novo(s), {(~is_new).sum()} modificado(s))."
            )
            log_group(
                f"Arquivos para processar ({len(pending)})",
                [
                    f"{row.FileName}  "
                    + (
                        "novo"
                        if new
                        else f"modificado (silver de {row.LastModified_y:%Y-%m-%d %H:%M} UTC)"
                    )
                    for row, new in zip(pending.itertuples(), is_new)
                ],
            )

        return files_to_records(
            pending.rename(columns={"LastModified_x": "LastModified"})
        )

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
