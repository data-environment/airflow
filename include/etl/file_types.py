import io
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa
from pandas import DataFrame

from include.aws.s3.client import S3Client


@dataclass
class Parquet:
    s3: S3Client
    schema: pa.Schema | None
    extension = "parquet"

    def get(self, bucket: str, key: str, engine="pyarrow", **kwargs) -> DataFrame:
        buffer = io.BytesIO(self.s3.get_bytes(bucket, key))
        return pd.read_parquet(buffer, engine=engine, **kwargs)

    def put(self, dataframe: DataFrame, bucket: str, prefix: str, file_name: str):
        with io.BytesIO() as buffer:
            dataframe.to_parquet(
                buffer,
                engine="pyarrow",
                index=False,
                compression="gzip",
                schema=self.schema,
            )
            return self.s3.put_bytes(
                buffer.getvalue(), bucket, f"{prefix}/{file_name}.parquet"
            )


@dataclass
class CSV:
    s3: S3Client
    encoding: str = "UTF-8"
    sep: str = ","
    errors: str = "strict"
    extension = "csv"

    def get(
        self, bucket: str, key: str, dtype=str, skip_blank_lines=True, **kwargs
    ) -> DataFrame:
        return pd.read_csv(
            self.s3.get_body(bucket, key),
            dtype=dtype,
            sep=self.sep,
            encoding=self.encoding,
            skip_blank_lines=skip_blank_lines,
            **kwargs,
        )

    def put(self, dataframe: DataFrame, bucket: str, prefix: str, file_name: str):
        with io.BytesIO() as buffer:
            dataframe.to_csv(
                buffer,
                index=False,
                sep=self.sep,
                encoding=self.encoding,
                errors=self.errors,
            )
            return self.s3.put_bytes(
                buffer.getvalue(), bucket, f"{prefix}/{file_name}.csv"
            )


@dataclass
class XLSX:
    s3: S3Client
    header: int = 0
    extension = "xlsx"

    def get(self, bucket: str, key: str, dtype=str, **kwargs) -> DataFrame:
        buffer = io.BytesIO(self.s3.get_bytes(bucket, key))
        return pd.read_excel(
            buffer, engine="openpyxl", header=self.header, dtype=dtype, **kwargs
        )

    def put(self, dataframe: DataFrame, bucket: str, prefix: str, file_name: str):
        with io.BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                dataframe.to_excel(writer, sheet_name="Planilha1", index=False)
            return self.s3.put_bytes(
                buffer.getvalue(), bucket, f"{prefix}/{file_name}.xlsx"
            )


@dataclass
class HTML:
    s3: S3Client
    extension = "html"

    def get(self, bucket: str, key: str, **kwargs) -> DataFrame:
        return pd.read_html(io.BytesIO(self.s3.get_bytes(bucket, key)))[0]


@dataclass
class XLS:
    s3: S3Client
    extension = "xls"

    def get(self, bucket: str, key: str, dtype=str, **kwargs) -> DataFrame:
        buffer = io.BytesIO(self.s3.get_bytes(bucket, key))
        return pd.read_excel(buffer, engine="xlrd", dtype=dtype, **kwargs)


FILE_TYPES = {"parquet": Parquet, "csv": CSV, "xlsx": XLSX, "html": HTML, "xls": XLS}


def build_file_type(spec: dict, s3: S3Client, schema: pa.Schema | None):
    """Instancia um formato a partir do dicionário do YAML."""
    options = dict(spec)
    file_type = options.pop("type")
    if file_type not in FILE_TYPES:
        raise ValueError(
            f"Formato '{file_type}' não suportado. Use um de: {', '.join(FILE_TYPES)}"
        )
    if file_type == "parquet":
        return Parquet(s3, schema=schema)
    return FILE_TYPES[file_type](s3, **options)
