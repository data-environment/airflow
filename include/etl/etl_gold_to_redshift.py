import logging
from datetime import date
from typing import TYPE_CHECKING

from include.etl.contract import Contract, file_date

if TYPE_CHECKING:
    from include.aws.redshift.client import RedshiftClient
    from include.etl.layer import Layer

logger = logging.getLogger(__name__)


class GoldToRedshift:
    """Carrega um arquivo da gold no Redshift (`load_mode: replace_partition`).

    Parquet → staging (tipos do schema físico do arquivo) → COPY →
    DELETE do dt_ref + INSERT na tabela final, numa única transação.
    A staging é por arquivo e é sempre apagada no final.
    """

    def __init__(
        self,
        contract: Contract,
        layer: "Layer | None" = None,
        client: "RedshiftClient | None" = None,
    ):
        self.contract = contract
        self.layer = layer or contract.layer()
        self.redshift = contract.redshift
        self.client = client or self._default_client()

    def _default_client(self) -> "RedshiftClient":
        from include.aws.redshift.client import RedshiftClient
        from include.aws.redshift.connection import RedshiftConnection

        return RedshiftClient(RedshiftConnection(database=self.redshift.database))

    def loaded_dt_refs(self) -> list[dict]:
        """`dt_ref` já carregados na tabela de destino e quando foram
        carregados pela última vez (`MAX(dt_relatorio)`), no formato do XCom."""
        import pandas as pd

        table = self.redshift.qualified_table
        loaded = self.client.get(
            f"SELECT dt_ref, MAX(dt_relatorio) AS dt_relatorio "
            f"FROM {table} GROUP BY dt_ref"
        )

        if loaded.empty:
            logger.warning(f"Redshift: {table} sem nenhum dt_ref carregado.")
        else:
            logger.info(
                f"Redshift: {len(loaded)} dt_ref carregado(s) em "
                f"{self.redshift.database}.{table} "
                f"(de {loaded['dt_ref'].min()} a {loaded['dt_ref'].max()})."
            )
        return [
            {
                "dt_ref": str(row.dt_ref),
                "dt_relatorio": pd.Timestamp(row.dt_relatorio).isoformat(),
            }
            for row in loaded.itertuples()
        ]

    def run(self, file_name: str, gold_key: str, tipo: str | None = None) -> dict:
        schema, table = self.redshift.schema_name, self.redshift.table
        dt_ref = file_date(file_name)
        staging_table = self.staging_table(dt_ref)

        business_columns = self.client.get_business_columns(schema, table)
        staging_columns = self._staging_columns(gold_key, business_columns)

        try:
            self.client.create_staging_table(schema, staging_table, staging_columns)
            rows = self.client.copy_into_staging(
                schema,
                staging_table,
                f"s3://{self.layer.settings.bucket_gold}/{gold_key}",
                staging_columns,
            )
            deleted = self.client.replace_partition_from_staging(
                schema, table, staging_table, business_columns, f"dt_ref = '{dt_ref}'"
            )
        finally:
            self.client.drop_table(schema, staging_table)

        logger.info(
            f"{file_name} ({tipo}): {rows} linha(s) carregada(s) em "
            f"{self.redshift.database}.{schema}.{table} para dt_ref {dt_ref} "
            f"({deleted} linha(s) anteriores substituída(s))."
        )
        return {
            "FileName": file_name,
            "dt_ref": str(dt_ref),
            "rows": rows,
            "deleted": deleted,
        }

    def staging_table(self, dt_ref: date) -> str:
        return f"stg_{self.redshift.table}_{dt_ref:%Y%m%d}"

    def _staging_columns(self, gold_key: str, business_columns: list) -> list:
        """Colunas da staging a partir do schema físico do Parquet, validadas
        contra as colunas de negócio da tabela final."""
        schema, table = self.redshift.schema_name, self.redshift.table
        staging_columns = self.client.build_staging_columns(
            self.layer.s3_client.get_parquet_schema(
                self.layer.settings.bucket_gold, gold_key
            ),
            self.client.get_varchar_length_hints(schema, table),
        )
        self.client.validate_columns_match(business_columns, staging_columns)
        return staging_columns
