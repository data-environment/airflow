import logging
from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame

from include.audit import mail
from include.config import TIMEZONE
from include.etl.contract import Contract, file_date

logger = logging.getLogger(__name__)


def bronze_by_dt_ref(contract: Contract) -> DataFrame:
    """`dt_ref` na bronze e quando o arquivo mais recente daquele dt_ref
    chegou (`LastModified` em Brasília sem fuso). Com CNPJ ou partição, vale
    o último arquivo do dt_ref."""
    files = contract.layer().get_bronze_files()
    return (
        files.assign(
            dt_ref=files["FileName"].map(file_date),
            LastModified=pd.to_datetime(files["LastModified"], utc=True)
            .dt.tz_convert(TIMEZONE)
            .dt.tz_localize(None),
        )
        .groupby("dt_ref", as_index=False)["LastModified"]
        .max()
    )


def redshift_by_dt_ref(contract: Contract) -> DataFrame:
    """`dt_ref` carregados no Redshift e quando foram carregados pela última
    vez (`dt_relatorio`, em Brasília sem fuso)."""
    from include.etl.etl_gold_to_redshift import GoldToRedshift

    loaded = pd.DataFrame(
        GoldToRedshift(contract).loaded_dt_refs(), columns=["dt_ref", "dt_relatorio"]
    )
    loaded["dt_ref"] = pd.to_datetime(loaded["dt_ref"]).dt.date
    loaded["dt_relatorio"] = pd.to_datetime(loaded["dt_relatorio"])
    if loaded["dt_relatorio"].dt.tz is not None:
        loaded["dt_relatorio"] = (
            loaded["dt_relatorio"].dt.tz_convert(TIMEZONE).dt.tz_localize(None)
        )
    return loaded


def check(contract: Contract, now: datetime) -> dict | None:
    """dt_ref da bronze que, passado o `alerts.redshift_delay.sla` desde a
    chegada do arquivo, não estão no Redshift (`missing`) ou foram carregados
    antes da última versão do arquivo (`outdated`). None se está tudo em dia."""
    deadline = contract.sla.deadline if contract.sla else timedelta(0)
    compare = pd.merge(
        bronze_by_dt_ref(contract),
        redshift_by_dt_ref(contract),
        on="dt_ref",
        how="left",
    )
    overdue = compare["LastModified"] + deadline <= now
    is_missing = compare["dt_relatorio"].isna()
    is_outdated = compare["LastModified"] > compare["dt_relatorio"]
    pending = compare[overdue & (is_missing | is_outdated)].sort_values("dt_ref")

    logger.info(
        f"{contract.pipeline}: {len(compare)} dt_ref na bronze, "
        f"{len(pending)} fora do SLA no Redshift ({contract.sla or 'sem SLA'})"
    )
    if pending.empty:
        return None

    alert = contract.alerts.redshift_delay
    return {
        "pipeline": contract.pipeline,
        "display_name": contract.display_name,
        "table": f"{contract.redshift.database}.{contract.redshift.qualified_table}",
        "sla": str(contract.sla) if contract.sla else None,
        "to": alert.to if alert else [],
        "cc": alert.cc if alert else [],
        "files": [
            {
                "dt_ref": row.dt_ref.isoformat(),
                "status": "missing" if pd.isna(row.dt_relatorio) else "outdated",
                "bronze_at": row.LastModified.isoformat(),
                "loaded_at": (
                    None
                    if pd.isna(row.dt_relatorio)
                    else row.dt_relatorio.isoformat()
                ),
            }
            for row in pending.itertuples()
        ],
    }


def send_report(divergences: list[dict], now: datetime) -> None:
    """Um único email com todas as divergências, para os responsáveis
    (`alerts.redshift_delay`) de todos os pipelines com divergência."""
    from include.audit.email_template import redshift_reconciliation_email

    to, cc = [], []
    for divergence in divergences:
        if not divergence["to"]:
            logger.warning(
                f"{divergence['pipeline']} com divergência no Redshift, mas sem "
                "`alerts.redshift_delay.to`; vai no email sem responsável."
            )
        to += [email for email in divergence["to"] if email not in to]
        cc += [email for email in divergence["cc"] if email not in cc]
    cc = [email for email in cc if email not in to]

    if not to:
        logger.error(
            "Nenhum destinatário em `alerts.redshift_delay.to`; email não enviado."
        )
        return

    subject, html = redshift_reconciliation_email(divergences, now)
    mail.send(to, cc, subject, html)
