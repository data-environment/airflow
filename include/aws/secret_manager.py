import json
import logging
import os

import boto3
import dotenv

logger = logging.getLogger(__name__)


def load_config() -> None:
    """SM e fonte primaria. .env cobre apenas as chaves que SM nao forneceu."""

    secret_name = os.environ.get("SECRET_NAME")
    region = os.environ.get("AWS_REGION") or os.environ.get(
        "AWS_DEFAULT_REGION", "us-east-1"
    )
    try:
        client = boto3.client(
            "secretsmanager",
            region_name=region,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        response = client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response["SecretString"])
        for key, value in secrets.items():
            os.environ[key] = str(value)
    except Exception as exc:
        logger.warning(
            f"Falha ao ler o secret '{secret_name}' do Secrets Manager; "
            f"usando apenas variáveis de ambiente: {exc}"
        )
    finally:
        dotenv.load_dotenv(".env", override=False)
