import logging

from pandas import DataFrame

from include.etl.layer import LayerUtils, files_to_records, records_to_files

logger = logging.getLogger(__name__)

MAX_LOGGED_FILES = 20


def log_group(title: str, lines: list[str]) -> None:
    """Loga `lines` entre `::group::`/`::endgroup::`, que a UI do Airflow
    mostra como uma seção recolhível. Limita a `MAX_LOGGED_FILES` linhas."""
    logger.info(f"::group::{title}")
    for line in lines[:MAX_LOGGED_FILES]:
        logger.info(line)
    if len(lines) > MAX_LOGGED_FILES:
        logger.info(f"... e mais {len(lines) - MAX_LOGGED_FILES}")
    logger.info("::endgroup::")


def log_files(
    layer_name: str, bucket: str, prefixes: list[str], files: DataFrame
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
        f"Arquivos na {layer_name} ({len(files)})",
        [
            f"{row.FileName}  modificado em {row.LastModified:%Y-%m-%d %H:%M} UTC  "
            f"{row.Size / 1024:,.0f} KB"
            for row in files.itertuples()
        ],
    )


def compare_layers(
    source_name: str, source: list[dict], target_name: str, target: list[dict]
) -> list[dict]:
    """Arquivos de `source` novos ou modificados em relação a `target`,
    no formato do XCom, com o `LastModified` do `source`."""
    pending = LayerUtils().compare(records_to_files(source), records_to_files(target))
    is_new = pending["LastModified_y"].isna()
    label = f"{source_name.capitalize()} x {target_name}"

    if pending.empty:
        logger.info(f"{label}: nenhum arquivo novo ou modificado.")
    else:
        logger.info(
            f"{label}: {len(pending)} arquivo(s) para processar "
            f"({is_new.sum()} novo(s), {(~is_new).sum()} modificado(s))."
        )
        log_group(
            f"Arquivos para processar ({len(pending)})",
            [
                f"{row.FileName}  "
                + (
                    "novo"
                    if new
                    else f"modificado ({target_name} de {row.LastModified_y:%Y-%m-%d %H:%M} UTC)"
                )
                for row, new in zip(pending.itertuples(), is_new)
            ],
        )

    return files_to_records(pending.rename(columns={"LastModified_x": "LastModified"}))
