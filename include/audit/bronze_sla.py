import logging
from collections import defaultdict
from datetime import date

from include.audit import mail
from include.etl.contract import Contract, file_date

logger = logging.getLogger(__name__)


def latest_dt_ref(contract: Contract) -> date | None:
    """dt_ref mais recente na bronze (o `AAAAMMDD` no nome do arquivo)."""
    files = contract.layer().get_bronze_files()
    return max((file_date(name) for name in files["FileName"]), default=None)


def check(contract: Contract, today: date) -> dict | None:
    """Atraso da bronze em relação a `alerts.s3_delay.max_days_without_update`,
    ou None se o contrato está dentro do prazo (ou não tem esse alerta)."""
    alert = contract.alerts.s3_delay
    if not alert or not alert.max_days_without_update:
        return None

    last = latest_dt_ref(contract)
    days = (today - last).days if last else None
    logger.info(
        f"{contract.pipeline}: último dt_ref {last}, {days} dia(s) sem atualização, "
        f"limite {alert.max_days_without_update}"
    )
    if days is not None and days <= alert.max_days_without_update:
        return None

    return {
        "pipeline": contract.pipeline,
        "display_name": contract.display_name,
        "last_update": last.isoformat() if last else None,
        "days_without_update": days,
        "limit_days": alert.max_days_without_update,
        "to": alert.to,
        "cc": alert.cc,
    }


def send_report(delays: list[dict], today: date) -> None:
    """Um email por grupo de destinatários (`to` + `cc`) com os arquivos
    atrasados do grupo. Fora de prod, tudo vai só para `DEV_RECIPIENT` (.env)."""
    from include.audit.email_template import bronze_sla_email

    groups = defaultdict(list)
    for delay in delays:
        if not delay["to"]:
            logger.warning(
                f"{delay['pipeline']} atrasado na bronze, mas sem "
                "`alerts.s3_delay.to`; email não enviado."
            )
            continue
        groups[(tuple(delay["to"]), tuple(delay["cc"]))].append(delay)

    for (to, cc), group in groups.items():
        subject, html = bronze_sla_email(group, today)
        logger.info(
            f"SLA da bronze para {list(to)}: "
            f"{', '.join(delay['pipeline'] for delay in group)}"
        )
        mail.send(list(to), list(cc), subject, html)
