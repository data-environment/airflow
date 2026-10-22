from datetime import date, datetime

MONTHS = [
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
]

TH = (
    "padding:8px 12px;font-size:11px;color:#999999;text-align:left;"
    "text-transform:uppercase;letter-spacing:1px;border-bottom:2px solid #eeeeee;"
)
TD = "padding:10px 12px;font-size:14px;color:#555555;border-bottom:1px solid #eeeeee;"
TD_FILE = (
    "padding:10px 12px;font-size:14px;color:#222222;font-weight:600;"
    "font-family:monospace,Courier New;border-bottom:1px solid #eeeeee;"
)


def _days(days: int) -> str:
    return f"{days} {'dia' if days == 1 else 'dias'}"


def _format_date(value: str | None) -> str:
    if not value:
        return "Nenhum arquivo encontrado"
    dt = date.fromisoformat(value)
    return f"{dt.day} de {MONTHS[dt.month - 1]} de {dt.year}"


def _format_datetime(value: str | None) -> str:
    if not value:
        return "—"
    return datetime.fromisoformat(value).strftime("%d/%m/%Y %H:%M")


def _row(delay: dict) -> str:
    elapsed = delay["days_without_update"]
    return (
        "<tr>"
        f'<td style="{TD_FILE}">{delay["display_name"]}</td>'
        f'<td style="{TD}">{_format_date(delay["last_update"])}</td>'
        f'<td style="{TD}">{_days(elapsed) if elapsed is not None else "—"}</td>'
        f'<td style="{TD}">{_days(delay["limit_days"])}</td>'
        "</tr>"
    )


def _wrap_html(title: str, body: str) -> str:
    return f"""<html>
<body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f4f4f4;">
    <tr>
      <td align="center" style="padding:32px 16px;">
        <table width="600" cellpadding="0" cellspacing="0" border="0"
               style="max-width:600px;width:100%;background:#ffffff;border-radius:8px;
                      overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08);">
          <tr>
            <td style="background:#c0392b;padding:24px 32px;">
              <p style="margin:0 0 4px;font-size:11px;color:rgba(255,255,255,0.75);
                        font-weight:600;letter-spacing:2px;text-transform:uppercase;">
                Sistema de Monitoramento · InvestSmart
              </p>
              <h1 style="margin:0;font-size:20px;color:#ffffff;font-weight:700;line-height:1.3;">
                &#9888;&#65039; {title}
              </h1>
            </td>
          </tr>
          <tr>
            <td style="padding:32px;">
              {body}
            </td>
          </tr>
          <tr>
            <td style="background:#fafafa;padding:16px 32px;border-top:1px solid #eeeeee;">
              <p style="margin:0;font-size:11px;color:#aaaaaa;text-align:center;">
                Data Warehouse · dw@investsmart.com.br
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def bronze_sla_email(delays: list[dict], today: date) -> tuple[str, str]:
    """(assunto, html) do relatório de arquivos fora do SLA da bronze."""
    total = len(delays)
    label = "arquivo atrasado" if total == 1 else "arquivos atrasados"
    rows = "".join(
        _row(delay) for delay in sorted(delays, key=lambda d: d["display_name"])
    )
    body = f"""
<p style="margin:0 0 20px;font-size:15px;color:#555555;line-height:1.6;">
  A auditoria de {today.strftime("%d/%m/%Y")} encontrou <b>{total}</b> {label}
  na camada Bronze (S3), sem atualização dentro do prazo esperado.
</p>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">
  <tr>
    <th style="{TH}">Arquivo</th>
    <th style="{TH}">Última modificação</th>
    <th style="{TH}">Tempo sem atualização</th>
    <th style="{TH}">Limite</th>
  </tr>
  {rows}
</table>
""".strip()

    return (
        f"⚠️ SLA Bronze XP — {total} {label}",
        _wrap_html("Atraso na Atualização — S3 Bronze", body),
    )


STATUS = {"missing": "Ausente no Redshift", "outdated": "Desatualizado"}


def _divergence_section(divergence: dict) -> str:
    rows = "".join(
        "<tr>"
        f'<td style="{TD_FILE}">{date.fromisoformat(file["dt_ref"]):%d/%m/%Y}</td>'
        f'<td style="{TD}">{STATUS[file["status"]]}</td>'
        f'<td style="{TD}">{_format_datetime(file["bronze_at"])}</td>'
        f'<td style="{TD}">{_format_datetime(file["loaded_at"])}</td>'
        "</tr>"
        for file in divergence["files"]
    )
    owners = ", ".join(divergence["to"]) or "sem responsável no contrato"
    return f"""
<div style="background:#fff5f5;border-left:4px solid #c0392b;padding:12px 16px;
            border-radius:4px;margin:28px 0 8px;">
  <p style="margin:0 0 4px;font-size:16px;color:#222222;font-weight:700;">
    {divergence["display_name"]}
    <span style="font-size:12px;color:#999999;font-weight:400;">
      ({len(divergence["files"])} {"data" if len(divergence["files"]) == 1 else "datas"})
    </span>
  </p>
  <p style="margin:0;font-size:12px;color:#777777;">
    <span style="font-family:monospace,Courier New;">{divergence["table"]}</span>
    · SLA {divergence["sla"] or "não definido"} · Responsável: {owners}
  </p>
</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">
  <tr>
    <th style="{TH}">dt_ref</th>
    <th style="{TH}">Situação</th>
    <th style="{TH}">Chegou na bronze</th>
    <th style="{TH}">Carregado no Redshift</th>
  </tr>
  {rows}
</table>
""".strip()


def redshift_reconciliation_email(
    divergences: list[dict], now: datetime
) -> tuple[str, str]:
    """(assunto, html) do relatório único de divergências bronze x Redshift."""
    divergences = sorted(divergences, key=lambda d: d["display_name"])
    total = sum(len(divergence["files"]) for divergence in divergences)
    pipelines = len(divergences)
    label = "data divergente" if total == 1 else "datas divergentes"
    body = f"""
<p style="margin:0 0 8px;font-size:15px;color:#555555;line-height:1.6;">
  A reconciliação de {now:%d/%m/%Y %H:%M} encontrou <b>{total}</b> {label}
  em <b>{pipelines}</b> {"pipeline" if pipelines == 1 else "pipelines"}: arquivos
  que estão na camada Bronze (S3) e, passado o SLA, não foram carregados no
  Redshift ou foram carregados antes da última versão do arquivo.
</p>
{"".join(_divergence_section(divergence) for divergence in divergences)}
""".strip()

    return (
        f"⚠️ Reconciliação Bronze x Redshift XP — {total} {label}",
        _wrap_html("Reconciliação Bronze x Redshift", body),
    )
