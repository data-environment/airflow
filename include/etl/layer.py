import re
from dataclasses import dataclass, field

import pandas as pd
from pandas import DataFrame

from include.aws.s3.client import S3Client
from include.config import EtlSettings
from include.config import settings as default_settings

FILE_COLUMNS = ["Key", "LastModified", "FileName", "ano"]


def relative_path(key: str) -> str:
    """Caminho após `year=AAAA/`, sem extensão (ex.: `trade_idea_20260810` ou
    `cnpj=.../acao/diversificacao_rv_20260810`). Igual entre bronze e silver."""
    return re.split(r"/year=\d{4}/", key, maxsplit=1)[-1].rsplit(".", 1)[0]


def files_to_records(files: DataFrame) -> list[dict]:
    """DataFrame de arquivos -> lista de dicts serializável em JSON (XCom)."""
    records = files[FILE_COLUMNS].copy()
    records["LastModified"] = pd.to_datetime(records["LastModified"], utc=True).map(
        lambda ts: ts.isoformat()
    )
    return records.to_dict("records")


def records_to_files(records: list[dict]) -> DataFrame:
    """Inverso de `files_to_records`: `LastModified` volta a ser datetime (UTC)."""
    files = pd.DataFrame(records, columns=FILE_COLUMNS)
    files["LastModified"] = pd.to_datetime(files["LastModified"], utc=True)
    return files


@dataclass
class Layer:
    path: str
    file: str
    cnpj: bool = False
    partitioned: bool = False
    settings: EtlSettings = field(default_factory=lambda: default_settings)
    s3_client: S3Client = field(default_factory=S3Client)

    def _is_within_first_quarter(self) -> bool:
        return self.settings.month <= 3

    def _get_files(self, prefix: str, bucket: str) -> DataFrame:
        response_json = self.s3_client.list_files(prefix, bucket)
        if response_json["KeyCount"] == 0:
            return pd.DataFrame(columns=FILE_COLUMNS)

        df_response = pd.json_normalize(response_json["Contents"])
        df_response["FileName"] = df_response["Key"].apply(
            lambda key: key.split("/")[-1].split(".")[0]
        )
        df_response["ano"] = df_response["FileName"].str[-8:-4]

        return df_response

    def year_prefix(self, year: int | str | None = None) -> str:
        year = self.settings.year if year is None else year
        return f"{self.settings.environment}/{self.path}/year={year}"

    def gold_prefix(self) -> str:
        return f"{self.settings.environment}/{self.path}"

    def file_prefixes(self) -> list[str]:
        """Prefixos pesquisados na bronze e na silver: o ano corrente e, no
        primeiro trimestre, também o anterior; um por CNPJ se `cnpj`. Se
        `partitioned`, o prefixo para na pasta (a listagem desce nas
        subpastas de partição)."""
        years = [self.settings.year]
        if self._is_within_first_quarter():
            years.insert(0, self.settings.year - 1)
        cnpjs = (
            [f"/cnpj={self.settings.cnpj_is_1}", f"/cnpj={self.settings.cnpj_is_2}"]
            if self.cnpj
            else [""]
        )
        end = "/" if self.partitioned else f"/{self.file}"
        return [
            f"{self.year_prefix(year)}{cnpj}{end}" for year in years for cnpj in cnpjs
        ]

    def _get_files_in(self, prefixes: list[str], bucket: str) -> DataFrame:
        files = pd.concat(
            [self._get_files(prefix, bucket) for prefix in prefixes],
            ignore_index=True,
        )
        is_file = files["FileName"].str.startswith(self.file)
        return files[is_file].reset_index(drop=True)

    def get_bronze_files(self) -> DataFrame:
        return self._get_files_in(self.file_prefixes(), self.settings.bucket_bronze)

    def get_silver_files(self) -> DataFrame:
        return self._get_files_in(self.file_prefixes(), self.settings.bucket_silver)

    def gold_file_prefixes(self) -> list[str]:
        """Prefixos pesquisados na gold (sem particionamento por ano)."""
        return [f"{self.gold_prefix()}/{self.file}"]

    def get_gold_files(self) -> DataFrame:
        return self._get_files_in(self.gold_file_prefixes(), self.settings.bucket_gold)
