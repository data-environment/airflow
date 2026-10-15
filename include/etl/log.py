import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pandas import DataFrame

MAX_LOGGED_LINES = 20


def log_group(logger: logging.Logger, title: str, lines: list[str]) -> None:
    """Loga `lines` entre `::group::`/`::endgroup::`, que a UI do Airflow
    mostra como uma seção recolhível. Limita a `MAX_LOGGED_LINES` linhas."""
    logger.info(f"::group::{title}")
    for line in lines[:MAX_LOGGED_LINES]:
        logger.info(line)
    if len(lines) > MAX_LOGGED_LINES:
        logger.info(f"... e mais {len(lines) - MAX_LOGGED_LINES}")
    logger.info("::endgroup::")


def log_files(
    logger: logging.Logger,
    layer_name: str,
    bucket: str,
    prefixes: list[str],
    files: "DataFrame",
) -> None:
    """Loga onde a camada foi pesquisada e os arquivos encontrados."""
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
