from dataclasses import dataclass, field

import pandas as pd
import pyarrow as pa
from pandas import DataFrame
from sqlalchemy import text

from include.aws.redshift.connection import RedshiftConnection
from include.config import settings

DEFAULT_STAGING_VARCHAR_LENGTH = 256


def map_arrow_field_to_redshift_type(field: pa.Field, varchar_hints: dict) -> str:
    """Mapeia um campo do schema físico do Parquet para o tipo SQL do Redshift.

    COPY FORMAT AS PARQUET exige que o tipo declarado na staging bata
    exatamente com o tipo físico do arquivo (sem coerção implícita, ex.:
    double -> NUMERIC falha no COPY mesmo sendo um cast válido em SQL
    comum) -- por isso o mapeamento parte do tipo Arrow, nunca da DDL da
    tabela final.
    """
    arrow_type = field.type
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ" if arrow_type.tz else "TIMESTAMP"
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_int16(arrow_type):
        return "SMALLINT"
    if pa.types.is_int32(arrow_type):
        return "INTEGER"
    if pa.types.is_int64(arrow_type):
        return "BIGINT"
    if pa.types.is_float32(arrow_type):
        return "REAL"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE PRECISION"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        length = varchar_hints.get(field.name, DEFAULT_STAGING_VARCHAR_LENGTH)
        return f"VARCHAR({length})"
    raise RuntimeError(
        f"Tipo Arrow '{arrow_type}' da coluna '{field.name}' não mapeado em "
        "map_arrow_field_to_redshift_type."
    )


@dataclass
class RedshiftClient:
    connection: RedshiftConnection = field(default_factory=RedshiftConnection)

    def get(self, query: str) -> DataFrame:
        try:
            with self.connection.engine.begin() as conn:
                result = conn.execute(text(query))
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as exc:
            raise RuntimeError(f"Failed to run Redshift query: {exc}") from exc

    def insert(
        self,
        dataframe: DataFrame,
        table: str,
        schema: str,
        if_exists: str = "append",
        chunksize: int = 10000,
    ) -> None:
        try:
            with self.connection.engine.begin() as conn:
                dataframe.to_sql(
                    table,
                    conn,
                    schema=schema,
                    index=False,
                    if_exists=if_exists,
                    chunksize=chunksize,
                    method="multi",
                )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to insert data into {schema}.{table}: {exc}"
            ) from exc

    def copy(
        self,
        table: str,
        schema: str,
        s3_path: str,
        file_format: str = "PARQUET",
    ) -> None:
        statement = f"""
            COPY {schema}.{table}
            FROM '{s3_path}'
            CREDENTIALS 'aws_access_key_id={settings.aws_public_key};aws_secret_access_key={settings.aws_secret_key}'
            FORMAT AS {file_format}
        """
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(statement))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to copy {s3_path} into {schema}.{table}: {exc}"
            ) from exc

    def truncate(self, table: str, schema: str) -> None:
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        except Exception as exc:
            raise RuntimeError(f"Failed to truncate {schema}.{table}: {exc}") from exc

    def delete(self, table: str, schema: str, where: str) -> None:
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {schema}.{table} WHERE {where}"))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to delete from {schema}.{table} where {where}: {exc}"
            ) from exc

    def get_business_columns(self, schema: str, table: str) -> list:
        """Colunas sem column_default no catálogo -- IDENTITY e DEFAULT comum
        são reportados da mesma forma pelo Redshift. Usado só para validar o
        conjunto de colunas esperado pela staging, nunca ordem ou tipo."""
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
              AND column_default IS NULL
            ORDER BY ordinal_position
        """
        try:
            with self.connection.engine.connect() as conn:
                rows = conn.execute(
                    text(query), {"schema": schema, "table": table}
                ).fetchall()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read business columns of {schema}.{table}: {exc}"
            ) from exc

        if not rows:
            raise RuntimeError(
                f"Nenhuma coluna de negócio encontrada para {schema}.{table}."
            )
        return [row[0] for row in rows]

    def get_varchar_length_hints(self, schema: str, table: str) -> dict:
        """Tamanho declarado de VARCHAR na tabela final, usado só como
        sugestão quando o Arrow não carrega tamanho máximo de string."""
        query = """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
              AND data_type = 'character varying'
              AND character_maximum_length IS NOT NULL
        """
        try:
            with self.connection.engine.connect() as conn:
                rows = conn.execute(
                    text(query), {"schema": schema, "table": table}
                ).fetchall()
            return dict(rows)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read varchar length hints of {schema}.{table}: {exc}"
            ) from exc

    def build_staging_columns(
        self, arrow_schema: pa.Schema, varchar_hints: dict
    ) -> list:
        """Define as colunas e tipos da staging a partir do schema físico
        real do Parquet, na ordem física do arquivo -- é essa ordem que deve
        ser usada tanto na DDL da staging quanto na lista de colunas do
        COPY."""
        return [
            (field.name, map_arrow_field_to_redshift_type(field, varchar_hints))
            for field in arrow_schema
        ]

    def validate_columns_match(
        self, business_columns: list, staging_columns: list
    ) -> None:
        expected = set(business_columns)
        found = {name for name, _ in staging_columns}
        missing = expected - found
        extra = found - expected
        if missing or extra:
            raise RuntimeError(
                "Colunas do Parquet não batem com as colunas de negócio "
                f"esperadas. Faltando no arquivo: {sorted(missing) or 'nenhuma'}. "
                f"Sobrando no arquivo: {sorted(extra) or 'nenhuma'}."
            )

    def create_staging_table(
        self, schema: str, staging_table: str, columns: list
    ) -> None:
        columns_sql = ",\n    ".join(
            f"{name} {redshift_type}" for name, redshift_type in columns
        )
        ddl = f"""
            DROP TABLE IF EXISTS {schema}.{staging_table};
            CREATE TABLE {schema}.{staging_table}
            (
                {columns_sql}
            )
            DISTSTYLE EVEN
        """
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(ddl))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create staging table {schema}.{staging_table}: {exc}"
            ) from exc

    def copy_into_staging(
        self,
        schema: str,
        staging_table: str,
        s3_path: str,
        columns: list,
        file_format: str = "PARQUET",
    ) -> int:
        # Ordem aqui precisa ser a ordem física do arquivo -- COPY FORMAT AS
        # PARQUET casa por posição contra o arquivo, não por nome, mesmo com
        # lista de colunas explícita.
        column_list = ", ".join(name for name, _ in columns)
        statement = f"""
            COPY {schema}.{staging_table} ({column_list})
            FROM '{s3_path}'
            CREDENTIALS 'aws_access_key_id={settings.aws_public_key};aws_secret_access_key={settings.aws_secret_key}'
            FORMAT AS {file_format}
        """
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(statement))
                (count,) = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{staging_table}")
                ).fetchone()
            return count
        except Exception as exc:
            raise RuntimeError(
                f"Failed to copy {s3_path} into {schema}.{staging_table}: {exc}"
            ) from exc

    def insert_from_staging(
        self, schema: str, table: str, staging_table: str, business_columns: list
    ) -> None:
        # Aqui a ordem não importa -- INSERT normal casa por nome, e faz o
        # cast de tipo (ex. double -> NUMERIC) sem problema.
        column_list = ", ".join(business_columns)
        statement = f"""
            INSERT INTO {schema}.{table} ({column_list})
            SELECT {column_list}
            FROM {schema}.{staging_table}
        """
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(statement))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to insert {schema}.{staging_table} into {schema}.{table}: {exc}"
            ) from exc

    def replace_partition_from_staging(
        self,
        schema: str,
        table: str,
        staging_table: str,
        business_columns: list,
        where: str,
    ) -> int:
        """DELETE da partição + INSERT a partir da staging numa única transação:
        se o INSERT falhar, o DELETE é desfeito e a tabela fica como estava.
        Retorna o número de linhas apagadas."""
        column_list = ", ".join(business_columns)
        try:
            with self.connection.engine.begin() as conn:
                deleted = conn.execute(
                    text(f"DELETE FROM {schema}.{table} WHERE {where}")
                ).rowcount
                conn.execute(
                    text(
                        f"INSERT INTO {schema}.{table} ({column_list}) "
                        f"SELECT {column_list} FROM {schema}.{staging_table}"
                    )
                )
            return deleted
        except Exception as exc:
            raise RuntimeError(
                f"Failed to replace {schema}.{table} where {where} "
                f"from {schema}.{staging_table}: {exc}"
            ) from exc

    def drop_table(self, schema: str, staging_table: str) -> None:
        try:
            with self.connection.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{staging_table}"))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to drop table {schema}.{staging_table}: {exc}"
            ) from exc
