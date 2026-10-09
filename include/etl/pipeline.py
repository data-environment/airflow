"""Etapas do pipeline medalhão, uma por task da DAG.

Sem dependência do Airflow: cada método recebe/devolve tipos serializáveis em
JSON, para trafegar pelo XCom entre as tasks.
"""

import logging
from dataclasses import dataclass, field

from pandas import DataFrame

from include.etl.layer import Layer, files_to_records

logger = logging.getLogger(__name__)

MAX_LOGGED_FILES = 20


def log_files(layer_name: str, bucket: str, prefixes: list[str], files: DataFrame) -> None:
    """Loga onde a camada foi pesquisada e os arquivos encontrados.

    A lista fica entre `::group::`/`::endgroup::`, que a UI do Airflow mostra
    como uma seção recolhível.
    """
    label = layer_name.capitalize()
    for prefix in prefixes:
        logger.info(f"{label}: procurando em {bucket}/{prefix}")

    if files.empty:
        logger.warning(f"{label}: nenhum arquivo encontrado.")
        return

    logger.info(f"{label}: {len(files)} arquivo(s) encontrado(s).")
    logger.info(f"::group::Arquivos na {layer_name} ({len(files)})")
    for row in files.head(MAX_LOGGED_FILES).itertuples():
        logger.info(
            f"{row.FileName}  modificado em {row.LastModified:%Y-%m-%d %H:%M} UTC  "
            f"{row.Size / 1024:,.0f} KB"
        )
    if len(files) > MAX_LOGGED_FILES:
        logger.info(f"... e mais {len(files) - MAX_LOGGED_FILES}")
    logger.info("::endgroup::")


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
            self.layer.bronze_prefixes(),
            files,
        )
        return files_to_records(files)
