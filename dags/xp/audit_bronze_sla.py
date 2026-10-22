from pathlib import Path

import pendulum
from airflow.sdk import DAG

from include.audit.tasks import check_bronze_sla, send_bronze_sla_report
from include.etl.contract import load_contracts

CONTRACTS_DIR = Path(__file__).parent / "contracts"

with DAG(
    dag_id="xp_audit_bronze_sla",
    description="Verifica o SLA da bronze (`alerts.s3_delay` dos contratos) e avisa os responsáveis por email.",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["xp", "sla"],
    default_args={"owner": "dw", "retries": 1},
):
    contracts = [
        contract.to_xcom()
        for contract in load_contracts(CONTRACTS_DIR)
        if contract.alerts.s3_delay
    ]
    send_bronze_sla_report(check_bronze_sla.expand(contract=contracts))
