import logging
from typing import TYPE_CHECKING

from airflow.sdk import get_current_context, task

if TYPE_CHECKING:
    from pandas import DataFrame

    from include.etl.contract import Contract

logger = logging.getLogger(__name__)


def _contract(contract: dict) -> "Contract":
    """O contrato chega às tasks serializado (`Contract.to_xcom`)."""
    from include.etl.contract import Contract

    return Contract.model_validate(contract)


def log_files(
    layer_name: str, bucket: str, prefixes: list[str], files: "DataFrame"
) -> None:
    """Loga onde a camada foi pesquisada e os arquivos encontrados."""
    from include.etl.log import log_group

    label = layer_name.capitalize()
    for prefix in prefixes:
        logger.info(f"{label}: procurando em {bucket}/{prefix}")

    if files.empty:
        logger.warning(f"{label}: nenhum arquivo encontrado.")
        return

    logger.info(f"{label}: {len(files)} arquivo(s) encontrado(s).")
    log_group(
        logger,
        f"Arquivos na {layer_name} ({len(files)})",
        [
            f"{row.FileName}  modificado em {row.LastModified:%Y-%m-%d %H:%M} UTC  "
            f"{row.Size / 1024:,.0f} KB"
            for row in files.itertuples()
        ],
    )


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
    from include.etl.layer_comparator import LayerComparator

    return LayerComparator().between_layers("bronze", bronze, "silver", silver)


@task
def compare_silver_and_gold(silver: list[dict], gold: list[dict]) -> list[dict]:
    from include.etl.layer_comparator import LayerComparator

    return LayerComparator().between_layers("silver", silver, "gold", gold)


@task
def compare_gold_and_redshift(gold: list[dict], redshift: list[dict]) -> list[dict]:
    from include.etl.layer_comparator import LayerComparator

    return LayerComparator().with_redshift(gold, redshift)


@task(map_index_template="{{ file_name }}")
def etl_bronze_to_silver(contract: dict, record: dict) -> dict:
    from include.etl.etl_bronze_to_silver import BronzeToSilver

    get_current_context()["file_name"] = record["FileName"]
    return BronzeToSilver(_contract(contract)).run(record["FileName"], record["Key"])


@task(map_index_template="{{ file_name }}")
def etl_silver_to_gold(contract: dict, record: dict) -> dict:
    from include.etl.etl_silver_to_gold import SilverToGold

    get_current_context()["file_name"] = record["FileName"]
    return SilverToGold(_contract(contract)).run(record["FileName"], record["Key"])


@task(map_index_template="{{ file_name }}", max_active_tis_per_dagrun=1)
def etl_gold_to_redshift(contract: dict, record: dict) -> dict:
    from include.etl.etl_gold_to_redshift import GoldToRedshift

    get_current_context()["file_name"] = record["FileName"]
    return GoldToRedshift(_contract(contract)).run(
        record["FileName"], record["Key"], record.get("tipo")
    )
