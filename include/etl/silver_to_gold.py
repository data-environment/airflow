import logging
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
from pandas import DataFrame, Series

from include.etl.contract import Contract, GoldColumn, file_date

if TYPE_CHECKING:
    from include.etl.layer import Layer

logger = logging.getLogger(__name__)


class SilverToGold:
    DERIVED_COLUMNS = frozenset({"dt_ref", "id_calendario_dt_ref"})

    def __init__(self, contract: Contract, layer: "Layer | None" = None):
        self.contract = contract
        self.layer = layer or contract.layer()
        self.columns = contract.silver_to_gold.columns

    def run(self, file_name: str, silver_key: str) -> dict:
        from include.etl.file_types import Parquet, build_file_type

        settings, s3 = self.layer.settings, self.layer.s3_client

        version = self.contract.bronze_to_silver.version_for(file_name)
        reader = build_file_type(version.destination, s3)
        silver = reader.get(settings.bucket_silver, silver_key)
        gold = self.transform(silver, file_name)

        prefix = self.layer.gold_prefix()
        Parquet(s3, schema=self.arrow_schema()).put(
            gold, settings.bucket_gold, prefix, file_name
        )
        gold_key = f"{prefix}/{file_name}.parquet"

        logger.info(
            f"{file_name}: {len(silver)} linha(s) lida(s) da silver, "
            f"{len(gold)} gravada(s) em {settings.bucket_gold}/{gold_key} "
            f"(dt_ref {gold['dt_ref'].iloc[0] if len(gold) else '-'})"
        )
        return {"FileName": file_name, "Key": gold_key, "rows": len(gold)}

    def transform(self, silver: DataFrame, file_name: str) -> DataFrame:
        """Converte a silver (lida com dtype=str) para as colunas e tipos da gold."""
        self._check_columns(silver)

        dt_ref = file_date(file_name)
        gold = DataFrame(index=silver.index)
        for column in self.columns:
            name = column.name
            try:
                if name == "dt_ref":
                    gold[name] = dt_ref
                elif name == "id_calendario_dt_ref":
                    gold[name] = int(dt_ref.strftime("%Y%m%d"))
                else:
                    gold[name] = self._convert(silver[name], column)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"Falha ao converter a coluna '{name}' para {column.type}: {exc}"
                ) from exc
        return gold.reset_index(drop=True)

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [(column.name, pa.type_for_alias(column.type)) for column in self.columns]
        )

    def _check_columns(self, silver: DataFrame) -> None:
        missing = [
            column.name
            for column in self.columns
            if column.name not in self.DERIVED_COLUMNS
            and column.name not in silver.columns
        ]
        if missing:
            raise ValueError(
                f"Colunas do schema da gold ausentes na silver: {missing}. "
                f"Colunas da silver: {list(silver.columns)}"
            )

    @classmethod
    def _convert(cls, values: Series, column: GoldColumn) -> Series:
        type_name = column.type
        if type_name == "string":
            if column.only_digits:
                values = values.str.replace(r"\D", "", regex=True).replace("", None)
            return values
        if type_name.startswith(("float", "double")):
            return pd.to_numeric(values)
        if type_name.startswith(("int", "uint")):
            return pd.to_numeric(values).astype("Int64")
        if type_name.startswith("timestamp"):
            return cls._to_datetime(values)
        if type_name.startswith("date"):
            return cls._to_datetime(values).dt.date
        raise ValueError(f"Tipo '{type_name}' da coluna '{column.name}' não suportado.")

    @staticmethod
    def _to_datetime(values: Series) -> Series:
        """Aceita `dd/mm/aaaa[ hh:mm:ss]` (export XP) e ISO (`aaaa-mm-dd...`)."""
        text = values.str.strip()
        is_br = text.str.match(r"^\d{2}/\d{2}/\d{4}").fillna(False)
        result = Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
        if is_br.any():
            result[is_br] = pd.to_datetime(text[is_br], format="mixed", dayfirst=True)
        other = ~is_br & text.notna()
        if other.any():
            result[other] = pd.to_datetime(text[other], format="ISO8601")
        return result
