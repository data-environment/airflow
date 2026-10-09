from dataclasses import dataclass

from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine

from include.config import settings


@dataclass
class RdsConnection:
    host: str = settings.rds_host
    port: str = settings.rds_port
    user: str = settings.rds_user
    password: str = settings.rds_pass
    database: str = settings.rds_db

    def __post_init__(self) -> None:
        try:
            url = URL.create(
                "postgresql+psycopg2",
                username=self.user,
                password=self.password,
                host=self.host,
                port=int(self.port),
                database=self.database,
            )
            self.engine: Engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_reset_on_return="rollback",
                connect_args={"connect_timeout": 30, "sslmode": "require"},
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create RDS engine for database '{self.database}': {exc}"
            ) from exc
