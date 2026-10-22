from pathlib import Path

import pendulum
from airflow.sdk import DAG

from include.audit.tasks import (
    reconcile_bronze_and_redshift,
    send_redshift_reconciliation_report,
)
from include.etl.contract import load_contracts

CONTRACTS_DIR = Path(__file__).parent / "contracts"

with DAG(
    dag_id="xp_audit_redshift_reconciliation",
    description="Reconcilia os dt_ref da bronze com o Redshift dentro do SLA (`alerts.redshift_delay` dos contratos) e envia um único email aos responsáveis.",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["xp", "sla"],
    default_args={"owner": "dw", "retries": 1},
):
    contracts = [contract.to_xcom() for contract in load_contracts(CONTRACTS_DIR)]
    send_redshift_reconciliation_report(
        reconcile_bronze_and_redshift.expand(contract=contracts)
    )
