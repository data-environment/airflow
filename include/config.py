import os
from dataclasses import dataclass, field
from datetime import date, datetime

import pytz

from include.aws.secret_manager import load_config

load_config()

TIMEZONE = pytz.timezone("America/Sao_Paulo")


@dataclass
class AwsSettings:
    aws_public_key: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_REGION", default="us-east-1")


@dataclass
class AzureSettings:
    azure_client_id: str = os.getenv("AZURE_CLIENT_ID")
    azure_client_secret: str = os.getenv("AZURE_CLIENT_SECRET")
    azure_tenant_id: str = os.getenv("AZURE_TENANT_ID")


@dataclass
class RedshiftSettings:
    redshift_user: str = os.getenv("REDSHIFT_USER")
    redshift_pass: str = os.getenv("REDSHIFT_PASSWORD")
    redshift_db: str = os.getenv("REDSHIFT_DB")
    redshift_host: str = os.getenv("REDSHIFT_HOST")
    redshift_port: str = os.getenv("REDSHIFT_PORT", default="5439")


@dataclass
class RdsSettings:
    rds_user: str = os.getenv("RDS_USER")
    rds_pass: str = os.getenv("RDS_PASSWORD")
    rds_db: str = os.getenv("RDS_DB")
    rds_host: str = os.getenv("RDS_HOST")
    rds_port: str = os.getenv("RDS_PORT", default="5432")


@dataclass
class SlackSettings:
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")


@dataclass
class EtlSettings:
    environment: str = os.getenv("ENVIRONMENT", "dev")
    # Fora de prod, os emails de auditoria vão só para este endereço
    dev_recipient: str = os.getenv("DEV_RECIPIENT")

    bucket_bronze: str = os.getenv("S3_BUCKET_BRONZE")
    bucket_silver: str = os.getenv("S3_BUCKET_SILVER")
    bucket_gold: str = os.getenv("S3_BUCKET_GOLD")

    cnpj_is_1 = "19438577000108_is_1"
    cnpj_is_2 = "00570786000182_is_2"

    reference_date: date = field(default_factory=lambda: datetime.now(TIMEZONE).date())

    @property
    def year(self) -> int:
        return self.reference_date.year

    @property
    def month(self) -> int:
        return self.reference_date.month


@dataclass
class Settings(
    EtlSettings,
    AwsSettings,
    SlackSettings,
    AzureSettings,
    RedshiftSettings,
    RdsSettings,
):
    pass


settings = Settings()
