from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.response import StreamingBody

from include.aws.s3.connection import S3Connection


@dataclass
class S3Client:
    connection: S3Connection = field(default_factory=S3Connection)

    def list_files(self, prefix: str, bucket: str) -> dict:
        try:
            contents = []
            paginator = self.connection.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                contents.extend(page.get("Contents", []))

            return {"KeyCount": len(contents), "Contents": contents}
        except Exception as exc:
            raise RuntimeError(
                f"Failed to list files in {bucket}/{prefix}: {exc}"
            ) from exc

    def get_body(self, bucket: str, key: str) -> StreamingBody:
        try:
            response = self.connection.client.get_object(Bucket=bucket, Key=key)
            return response["Body"]
        except Exception as exc:
            raise RuntimeError(f"Failed to get {bucket}/{key}: {exc}") from exc

    def get_bytes(self, bucket: str, key: str) -> bytes:
        try:
            return self.get_body(bucket, key).read()
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to read {bucket}/{key}: {exc}") from exc

    def put_bytes(self, body: bytes, bucket: str, key: str) -> str:
        try:
            response = self.connection.client.put_object(
                Body=body, Bucket=bucket, Key=key
            )
            return response["ETag"]
        except Exception as exc:
            raise RuntimeError(f"Failed to put {bucket}/{key}: {exc}") from exc

    def get_parquet_schema(self, bucket: str, key: str) -> pa.Schema:
        try:
            filesystem = pafs.S3FileSystem(
                access_key=self.connection.aws_access_key_id,
                secret_key=self.connection.aws_secret_access_key,
                region=self.connection.region_name,
            )
            with filesystem.open_input_file(f"{bucket}/{key}") as file:
                return pq.ParquetFile(file).schema_arrow
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read parquet schema from {bucket}/{key}: {exc}"
            ) from exc
