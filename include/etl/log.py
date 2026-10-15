import logging

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
