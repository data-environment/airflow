from dataclasses import dataclass

import boto3
from botocore.client import BaseClient

from include.config import settings


@dataclass
class S3Connection:
    aws_access_key_id: str = settings.aws_public_key
    aws_secret_access_key: str = settings.aws_secret_key
    region_name: str = settings.aws_region

    def __post_init__(self) -> None:
        try:
            self.client: BaseClient = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to create S3 client: {exc}") from exc
