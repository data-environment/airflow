import logging
from datetime import datetime

from airflow.sdk import get_current_context, task

logger = logging.getLogger(__name__)


@task(map_index_template="{{ pipeline }}")
def check_bronze_sla(contract: dict) -> dict | None:
    from include.audit.bronze_sla import check
    from include.config import TIMEZONE
    from include.etl.contract import Contract

    spec = Contract.model_validate(contract)
    get_current_context()["pipeline"] = spec.pipeline
    return check(spec, datetime.now(TIMEZONE).date())


@task(trigger_rule="all_done")
def send_bronze_sla_report(delays: list[dict | None]) -> None:
    from include.audit.bronze_sla import send_report
    from include.config import TIMEZONE

    delays = [delay for delay in delays if delay]
    if not delays:
        logger.info("Todas as camadas bronze dentro do SLA.")
        return
    send_report(delays, datetime.now(TIMEZONE).date())


def _now():
    """Agora em Brasília sem fuso, como os horários comparados."""
    from include.config import TIMEZONE

    return datetime.now(TIMEZONE).replace(tzinfo=None)


@task(map_index_template="{{ pipeline }}")
def reconcile_bronze_and_redshift(contract: dict) -> dict | None:
    from include.audit.redshift_reconciliation import check
    from include.etl.contract import Contract

    spec = Contract.model_validate(contract)
    get_current_context()["pipeline"] = spec.pipeline
    return check(spec, _now())


@task(trigger_rule="all_done")
def send_redshift_reconciliation_report(divergences: list[dict | None]) -> None:
    from include.audit.redshift_reconciliation import send_report

    divergences = [divergence for divergence in divergences if divergence]
    if not divergences:
        logger.info("Bronze e Redshift reconciliados.")
        return
    send_report(divergences, _now())
