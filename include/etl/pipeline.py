import logging
from dataclasses import dataclass, field

from pandas import DataFrame

from include.etl.layer import Layer, LayerUtils, files_to_records, records_to_files

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


@dataclass
class MedallionPipeline:
    path: str
    file: str
    layer: Layer = field(init=False)

    def __post_init__(self) -> None:
        self.layer = Layer(self.path, self.file)

    @classmethod
    def from_contract(cls, contract: dict) -> "MedallionPipeline":
        return cls(path=contract["s3"]["path"], file=contract["s3"]["file"])

    def list_bronze_files(self) -> list[dict]:
        files = self.layer.get_bronze_files()
        log_files(
            "bronze",
            self.layer.settings.bucket_bronze,
            self.layer.file_prefixes(),
            files,
        )
        return files_to_records(files)

    def list_silver_files(self) -> list[dict]:
        files = self.layer.get_silver_files()
        log_files(
            "silver",
            self.layer.settings.bucket_silver,
            self.layer.file_prefixes(),
            files,
        )
        return files_to_records(files)

    def compare_bronze_and_silver(
        self, bronze: list[dict], silver: list[dict]
    ) -> list[dict]:
        """Arquivos da bronze sem correspondente na silver (novos) ou
        modificados depois da versão que está na silver."""
        pending = LayerUtils().compare(
            records_to_files(bronze), records_to_files(silver)
        )
        is_new = pending["LastModified_y"].isna()

        if pending.empty:
            logger.info("Bronze x silver: nenhum arquivo novo ou modificado.")
        else:
            logger.info(
                f"Bronze x silver: {len(pending)} arquivo(s) para processar "
                f"({is_new.sum()} novo(s), {(~is_new).sum()} modificado(s))."
            )
            log_group(
                f"Arquivos para processar ({len(pending)})",
                [
                    f"{row.FileName}  "
                    + (
                        "novo"
                        if new
                        else f"modificado (silver de {row.LastModified_y:%Y-%m-%d %H:%M} UTC)"
                    )
                    for row, new in zip(pending.itertuples(), is_new)
                ],
            )

        return files_to_records(
            pending.rename(columns={"LastModified_x": "LastModified"})
        )
