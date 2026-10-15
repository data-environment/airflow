import logging
from typing import TYPE_CHECKING

from airflow.sdk import get_current_context, task

if TYPE_CHECKING:
    from pandas import DataFrame

    from include.etl.contract import Contract

logger = logging.getLogger(__name__)

MAX_LOGGED_FILES = 20


def _contract(contract: dict) -> "Contract":
    """O contrato chega às tasks serializado (`Contract.to_xcom`)."""
    from include.etl.contract import Contract

    return Contract.model_validate(contract)


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
    layer_name: str, bucket: str, prefixes: list[str], files: "DataFrame"
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


def _compare_layers(
    source_name: str, source: list[dict], target_name: str, target: list[dict]
) -> list[dict]:
    """Arquivos de `source` novos ou modificados em relação a `target`,
    no formato do XCom, com o `LastModified` do `source`."""
    from include.etl.layer import LayerUtils, files_to_records, records_to_files

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


@task
def list_bronze_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records

    layer = _contract(contract).layer()
    files = layer.get_bronze_files()
    log_files("bronze", layer.settings.bucket_bronze, layer.file_prefixes(), files)
    return files_to_records(files)


@task
def list_silver_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records

    layer = _contract(contract).layer()
    files = layer.get_silver_files()
    log_files("silver", layer.settings.bucket_silver, layer.file_prefixes(), files)
    return files_to_records(files)


@task
def list_gold_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records

    layer = _contract(contract).layer()
    files = layer.get_gold_files()
    log_files("gold", layer.settings.bucket_gold, layer.gold_file_prefixes(), files)
    return files_to_records(files)


@task
def list_redshift_files(contract: dict) -> list[dict]:
    """`dt_ref` já carregados na tabela de destino e quando foram carregados
    pela última vez (`MAX(dt_relatorio)`)."""
    import pandas as pd

    from include.aws.redshift.client import RedshiftClient
    from include.aws.redshift.connection import RedshiftConnection

    redshift = _contract(contract).redshift
    table = redshift.qualified_table
    client = RedshiftClient(RedshiftConnection(database=redshift.database))
    loaded = client.get(
        f"SELECT dt_ref, MAX(dt_relatorio) AS dt_relatorio FROM {table} GROUP BY dt_ref"
    )

    if loaded.empty:
        logger.warning(f"Redshift: {table} sem nenhum dt_ref carregado.")
    else:
        logger.info(
            f"Redshift: {len(loaded)} dt_ref carregado(s) em "
            f"{redshift.database}.{table} "
            f"(de {loaded['dt_ref'].min()} a {loaded['dt_ref'].max()})."
        )
    return [
        {
            "dt_ref": str(row.dt_ref),
            "dt_relatorio": pd.Timestamp(row.dt_relatorio).isoformat(),
        }
        for row in loaded.itertuples()
    ]


@task
def compare_bronze_and_silver(bronze: list[dict], silver: list[dict]) -> list[dict]:
    """Arquivos da bronze sem correspondente na silver (novos) ou
    modificados depois da versão que está na silver."""
    return _compare_layers("bronze", bronze, "silver", silver)


@task
def compare_silver_and_gold(silver: list[dict], gold: list[dict]) -> list[dict]:
    """Arquivos da silver sem correspondente na gold (novos) ou
    modificados depois da versão que está na gold."""
    return _compare_layers("silver", silver, "gold", gold)


@task
def compare_gold_and_redshift(gold: list[dict], redshift: list[dict]) -> list[dict]:
    """Arquivos da gold a carregar: `insert` (dt_ref ainda não está na
    tabela) ou `update` (arquivo modificado depois da última carga)."""
    import pandas as pd

    from include.config import TIMEZONE
    from include.etl.layer import LayerUtils, records_to_files

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


@task(map_index_template="{{ file_name }}")
def etl_bronze_to_silver(contract: dict, record: dict) -> dict:
    from include.etl.bronze_to_silver import BronzeToSilver

    get_current_context()["file_name"] = record["FileName"]
    return BronzeToSilver(_contract(contract)).run(record["FileName"], record["Key"])


@task(map_index_template="{{ file_name }}")
def etl_silver_to_gold(contract: dict, record: dict) -> dict:
    from include.etl.silver_to_gold import SilverToGold

    get_current_context()["file_name"] = record["FileName"]
    return SilverToGold(_contract(contract)).run(record["FileName"], record["Key"])


@task(map_index_template="{{ file_name }}", max_active_tis_per_dagrun=1)
def etl_gold_to_redshift(contract: dict, record: dict) -> dict:
    """Carrega um arquivo da gold no Redshift (`load_mode: replace_partition`).

    Parquet → staging (tipos do schema físico do arquivo) → COPY →
    DELETE do dt_ref + INSERT na tabela final, numa única transação.
    A staging é por arquivo e é sempre apagada no final.
    """
    from include.aws.redshift.client import RedshiftClient
    from include.aws.redshift.connection import RedshiftConnection
    from include.etl.contract import file_date

    get_current_context()["file_name"] = record["FileName"]
    contract = _contract(contract)
    redshift = contract.redshift

    layer = contract.layer()
    settings = layer.settings
    schema, table = redshift.schema_name, redshift.table
    file_name, gold_key = record["FileName"], record["Key"]
    dt_ref = file_date(file_name)
    staging_table = f"stg_{table}_{dt_ref:%Y%m%d}"

    client = RedshiftClient(RedshiftConnection(database=redshift.database))
    business_columns = client.get_business_columns(schema, table)
    staging_columns = client.build_staging_columns(
        layer.s3_client.get_parquet_schema(settings.bucket_gold, gold_key),
        client.get_varchar_length_hints(schema, table),
    )
    client.validate_columns_match(business_columns, staging_columns)

    try:
        client.create_staging_table(schema, staging_table, staging_columns)
        rows = client.copy_into_staging(
            schema,
            staging_table,
            f"s3://{settings.bucket_gold}/{gold_key}",
            staging_columns,
        )
        deleted = client.replace_partition_from_staging(
            schema, table, staging_table, business_columns, f"dt_ref = '{dt_ref}'"
        )
    finally:
        client.drop_table(schema, staging_table)

    logger.info(
        f"{file_name} ({record.get('tipo')}): {rows} linha(s) carregada(s) em "
        f"{redshift.database}.{schema}.{table} para dt_ref {dt_ref} "
        f"({deleted} linha(s) anteriores substituída(s))."
    )
    return {
        "FileName": file_name,
        "dt_ref": str(dt_ref),
        "rows": rows,
        "deleted": deleted,
    }
