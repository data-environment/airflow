from dataclasses import dataclass, field

import pandas as pd
from pandas import DataFrame

from include.aws.s3.client import S3Client
from include.config import EtlSettings
from include.config import settings as default_settings

FILE_COLUMNS = ["Key", "LastModified", "FileName", "ano"]


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

    def _get_files_with_previous_year(self, prefix: str, bucket: str) -> DataFrame:
        """
        Retorna os arquivos do prefixo e, dentro do primeiro trimestre
        (janeiro a março), também os do ano anterior.
        """
        if not self._is_within_first_quarter():
            return self._get_files(prefix, bucket)

        prefix_ano_anterior = prefix.replace(
            f"year={self.settings.year}/", f"year={self.settings.year - 1}/"
        )
        return pd.concat(
            [
                self._get_files(prefix_ano_anterior, bucket),
                self._get_files(prefix, bucket),
            ],
            ignore_index=True,
        )

    def year_prefix(self, year: int | str | None = None) -> str:
        year = self.settings.year if year is None else year
        return f"{self.settings.environment}/{self.path}/year={year}"

    def gold_prefix(self) -> str:
        return f"{self.settings.environment}/{self.path}"

    def file_prefixes(self) -> list[str]:
        """Prefixos pesquisados na bronze e na silver: o ano corrente e, no
        primeiro trimestre, também o anterior."""
        prefix = f"{self.year_prefix()}/{self.file}"
        if not self._is_within_first_quarter():
            return [prefix]
        return [f"{self.year_prefix(self.settings.year - 1)}/{self.file}", prefix]

    def _get_files_in(self, prefixes: list[str], bucket: str) -> DataFrame:
        return pd.concat(
            [self._get_files(prefix, bucket) for prefix in prefixes],
            ignore_index=True,
        )

    def get_bronze_files(self) -> DataFrame:
        return self._get_files_in(self.file_prefixes(), self.settings.bucket_bronze)

    def get_silver_files(self) -> DataFrame:
        return self._get_files_in(self.file_prefixes(), self.settings.bucket_silver)

    def gold_file_prefixes(self) -> list[str]:
        """Prefixos pesquisados na gold (sem particionamento por ano)."""
        return [f"{self.gold_prefix()}/{self.file}"]

    def get_gold_files(self) -> DataFrame:
        return self._get_files_in(self.gold_file_prefixes(), self.settings.bucket_gold)

    def get_cnpj_files(self) -> DataFrame:
        frames = []
        for cnpj in [self.settings.cnpj_is_1, self.settings.cnpj_is_2]:
            prefix = (
                f"{self.settings.environment}/{self.path}/cnpjs/{cnpj}/"
                f"year={self.settings.year}/{self.file}"
            )
            frames.append(
                self._get_files_with_previous_year(prefix, self.settings.bucket_bronze)
            )
        return pd.concat(frames, ignore_index=True)

