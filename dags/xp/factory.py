import logging
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)

CONTRACTS_DIR = Path(__file__).parent / "contracts"
START_DATE = pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo")


def load_contracts() -> list[dict]:
    contracts = []
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        try:
            contracts.append(yaml.safe_load(path.read_text(encoding="utf-8")))
        except yaml.YAMLError as exc:
            logger.error(f"YAML inválido, DAG não criada: {path.name}: {exc}")
    return contracts


def create_dag(contract: dict) -> DAG:
    with DAG(
        dag_id=f"xp_{contract['pipeline']}",
        description=contract.get("description"),
        schedule=contract.get("schedule"),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["xp", *contract.get("tags", [])],
        default_args={
            "owner": contract.get("owner", "dw"),
            "retries": contract.get("retries", 0),
        },
    ) as dag:

        @task
        def list_bronze_files() -> list[dict]:
            from include.etl.pipeline import MedallionPipeline

            return MedallionPipeline.from_contract(contract).list_bronze_files()

        @task
        def list_silver_files() -> list[dict]:
            from include.etl.pipeline import MedallionPipeline

            return MedallionPipeline.from_contract(contract).list_silver_files()

        @task
        def compare_bronze_and_silver(
            bronze: list[dict], silver: list[dict]
        ) -> list[dict]:
            from include.etl.pipeline import MedallionPipeline

            return MedallionPipeline.from_contract(contract).compare_bronze_and_silver(
                bronze, silver
            )

        compare_bronze_and_silver(list_bronze_files(), list_silver_files())

    return dag


for _contract in load_contracts():
    try:
        create_dag(_contract)
    except Exception as exc:
        logger.error(f"DAG não criada para {_contract.get('pipeline')}: {exc}")
