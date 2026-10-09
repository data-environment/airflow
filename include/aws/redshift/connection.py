from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from include.config import settings


@dataclass
class RedshiftConnection:
    host: str = settings.redshift_host
    port: str = settings.redshift_port
    user: str = settings.redshift_user
    password: str = settings.redshift_pass
    database: str = "dw"

    def __post_init__(self) -> None:
        try:
            self.engine: Engine = create_engine(
                f"redshift+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_reset_on_return="rollback",
                connect_args={"connect_timeout": 30, "sslmode": "require"},
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create Redshift engine for database '{self.database}': {exc}"
            ) from exc
