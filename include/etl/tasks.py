import logging
from typing import TYPE_CHECKING

from airflow.sdk import get_current_context, task

if TYPE_CHECKING:
    from include.etl.contract import Contract

logger = logging.getLogger(__name__)


def _contract(contract: dict) -> "Contract":
    """O contrato chega às tasks serializado (`Contract.to_xcom`)."""
    from include.etl.contract import Contract

    return Contract.model_validate(contract)


@task
def list_bronze_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records
    from include.etl.log import log_files

    layer = _contract(contract).layer()
    files = layer.get_bronze_files()
    log_files(
        logger, "bronze", layer.settings.bucket_bronze, layer.file_prefixes(), files
    )
    return files_to_records(files)


@task
def list_silver_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records
    from include.etl.log import log_files

    layer = _contract(contract).layer()
    files = layer.get_silver_files()
    log_files(
        logger, "silver", layer.settings.bucket_silver, layer.file_prefixes(), files
    )
    return files_to_records(files)


@task
def list_gold_files(contract: dict) -> list[dict]:
    from include.etl.layer import files_to_records
    from include.etl.log import log_files

    layer = _contract(contract).layer()
    files = layer.get_gold_files()
    log_files(
        logger, "gold", layer.settings.bucket_gold, layer.gold_file_prefixes(), files
    )
    return files_to_records(files)


@task
def list_redshift_files(contract: dict) -> list[dict]:
    from include.etl.etl_gold_to_redshift import GoldToRedshift

    return GoldToRedshift(_contract(contract)).loaded_dt_refs()


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
