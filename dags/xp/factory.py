import logging
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG, TaskGroup
from pydantic import ValidationError

from include.etl.contract import Contract
from include.etl.tasks import (
    compare_bronze_and_silver,
    compare_gold_and_redshift,
    compare_silver_and_gold,
    etl_bronze_to_silver,
    etl_gold_to_redshift,
    etl_silver_to_gold,
    list_bronze_files,
    list_gold_files,
    list_redshift_files,
    list_silver_files,
    wait_for_bronze_files,
)

logger = logging.getLogger(__name__)

CONTRACTS_DIR = Path(__file__).parent / "contracts"
START_DATE = pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo")


def load_contracts() -> list[Contract]:
    contracts = []
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            contracts.append(Contract.model_validate(raw))
        except (yaml.YAMLError, ValidationError) as exc:
            logger.error(f"Contrato inválido, DAG não criada: {path.name}: {exc}")
    return contracts


def create_dag(spec: Contract) -> DAG:
    contract = spec.to_xcom()
    with DAG(
        dag_id=f"xp_{spec.pipeline}",
        description=spec.description,
        schedule="@continuous" if spec.sensor_interval else spec.schedule,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["xp", *spec.tags],
        default_args={"owner": spec.owner, "retries": spec.retries},
    ) as dag:

        with TaskGroup("bronze_to_silver"):
            bronze, silver = list_bronze_files(contract), list_silver_files(contract)
            if spec.sensor_interval:
                wait_for_bronze_files.override(poke_interval=spec.sensor_interval)(
                    contract
                ) >> [bronze, silver]
            pending = compare_bronze_and_silver(bronze, silver)
            transformed = etl_bronze_to_silver.partial(contract=contract).expand(
                record=pending
            )

        with TaskGroup("silver_to_gold"):
            silver, gold = list_silver_files(contract), list_gold_files(contract)
            transformed >> [silver, gold]
            pending = compare_silver_and_gold(silver, gold)
            transformed = etl_silver_to_gold.partial(contract=contract).expand(
                record=pending
            )

        with TaskGroup("gold_to_redshift"):
            gold, redshift = list_gold_files(contract), list_redshift_files(contract)
            transformed >> [gold, redshift]
            pending = compare_gold_and_redshift(gold, redshift)
            etl_gold_to_redshift.partial(contract=contract).expand(record=pending)

    return dag


for _contract in load_contracts():
    try:
        create_dag(_contract)
    except Exception as exc:
        logger.error(f"DAG não criada para {_contract.pipeline}: {exc}")
