from dataclasses import dataclass, field

import pandas as pd
import pytz
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


class LayerUtils:
    def __init__(self):
        self.brazil_timezone = pytz.timezone("America/Sao_Paulo")

    def compare(
        self,
        df_files_first_layer: DataFrame,
        df_files_second_layer: DataFrame,
    ) -> DataFrame:
        """Arquivos da primeira camada novos ou modificados em relação à segunda."""
        df_merged = pd.merge(
            df_files_first_layer[["FileName", "Key", "LastModified", "ano"]],
            df_files_second_layer[["FileName", "LastModified"]],
            on="FileName",
            how="left",
        )
        is_new = df_merged["LastModified_y"].isna()
        is_modified = df_merged["LastModified_x"] > df_merged["LastModified_y"]

        return df_merged[is_new | is_modified]

    def __treat_df_gold(self, df_gold: DataFrame) -> DataFrame:
        df_gold = df_gold.copy()
        df_gold["LastModified"] = df_gold["LastModified"].dt.tz_convert(
            self.brazil_timezone
        )
        df_gold["LastModified"] = df_gold["LastModified"].dt.tz_localize(None)
        df_gold["FileName"] = df_gold["FileName"].str[-8:].astype("Int64")

        return df_gold

    def __treat_df_redshift(self, df_redshift: DataFrame) -> DataFrame:
        df_redshift = df_redshift.copy()
        df_redshift["dt_ref"] = df_redshift["dt_ref"].astype("string")
        df_redshift["dt_ref"] = (
            df_redshift["dt_ref"].str.replace("-", "").astype("Int64")
        )

        return df_redshift

    def __create_df_etl(self, tb_comparacao: DataFrame) -> DataFrame:
        df_novos = tb_comparacao[tb_comparacao["dt_ref"].isnull()].copy()
        df_atualizacao = tb_comparacao[
            tb_comparacao["LastModified"] > tb_comparacao["dt_relatorio"]
        ].copy()
        df_atualizacao["tipo"] = "update"
        df_novos["tipo"] = "insert"

        return pd.concat([df_novos, df_atualizacao])

    def compare_with_redshift(
        self,
        df_layer: DataFrame,
        df_redshift: DataFrame,
    ) -> DataFrame:
        """Keys do gold a carregar: `insert` (dt_ref ausente) ou `update`
        (arquivo modificado depois do `dt_relatorio` carregado)."""
        if df_redshift.empty:
            df_layer = df_layer.copy()
            df_layer["tipo"] = "insert"
            return df_layer[["Key", "tipo"]]

        df_layer = self.__treat_df_gold(df_layer)
        df_redshift = self.__treat_df_redshift(df_redshift)
        tb_compare = pd.merge(
            df_layer, df_redshift, left_on="FileName", right_on="dt_ref", how="left"
        )
        df_etl = self.__create_df_etl(tb_compare)

        return df_etl[["Key", "tipo"]]
