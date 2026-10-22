import logging
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
from pandas import DataFrame, Series

from include.etl.contract import (
    Contract,
    EnrichmentColumn,
    GoldColumn,
    SilverToGoldVersion,
    file_date,
)

if TYPE_CHECKING:
    from include.etl.layer import Layer

logger = logging.getLogger(__name__)


class SilverToGold:
    def __init__(self, contract: Contract, layer: "Layer | None" = None):
        self.contract = contract
        self.layer = layer or contract.layer()

    def run(self, file_name: str, silver_keys: list[str]) -> dict:
        """Lê e concatena os arquivos da silver da data (um por CNPJ/partição,
        ou só um) e grava um único arquivo na gold."""
        from include.etl.file_types import Parquet, build_file_type
        from include.etl.layer import relative_path

        settings, s3 = self.layer.settings, self.layer.s3_client

        version = self.contract.bronze_to_silver.version_for(file_name)
        reader = build_file_type(version.destination, s3)
        frames = []
        for key in silver_keys:
            frame = reader.get(settings.bucket_silver, key)
            logger.info(f"{relative_path(key)}: {len(frame)} linha(s) na silver")
            frames.append(frame)
        silver = pd.concat(frames, ignore_index=True)
        gold = self.transform(silver, file_name)

        prefix = self.layer.gold_prefix()
        Parquet(s3, schema=self.arrow_schema(file_name)).put(
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
        version = self._version(file_name)
        self._check_columns(silver, version)

        dt_ref = file_date(file_name)
        enrichment = {column.name: column for column in version.enrichment}
        gold = DataFrame(index=silver.index)
        # Duas passadas: as colunas derivadas leem as colunas já convertidas.
        for derived in (False, True):
            for column in version.columns:
                name = column.name
                spec = enrichment.get(name)
                if (spec is not None and not spec.from_file_date) != derived:
                    continue
                try:
                    if spec is None:
                        gold[name] = self._convert(silver[name], column)
                    elif spec.from_file_date:
                        gold[name] = spec.value(dt_ref)
                    else:
                        source = (
                            self._convert(silver[name], column)
                            if spec.source == name
                            else gold[spec.source]
                        )
                        gold[name] = self._derive(gold, spec, source)
                except (ValueError, TypeError) as exc:
                    raise ValueError(
                        f"Falha ao converter a coluna '{name}' para {column.type}: {exc}"
                    ) from exc
        gold = gold[[column.name for column in version.columns]]
        return gold.reset_index(drop=True)

    def arrow_schema(self, file_name: str) -> pa.Schema:
        return pa.schema(
            [
                (column.name, pa.type_for_alias(column.type))
                for column in self._version(file_name).columns
            ]
        )

    def _version(self, file_name: str) -> SilverToGoldVersion:
        return self.contract.silver_to_gold.version_for(file_name)

    @staticmethod
    def _check_columns(silver: DataFrame, version: SilverToGoldVersion) -> None:
        # Enriquecimento que transforma a própria coluna também a lê da silver.
        enriched = {
            column.name
            for column in version.enrichment
            if column.source != column.name
        }
        missing = [
            column.name
            for column in version.columns
            if column.name not in enriched and column.name not in silver.columns
        ]
        if missing:
            raise ValueError(
                f"Colunas do schema da gold ausentes na silver: {missing}. "
                f"Colunas da silver: {list(silver.columns)}"
            )

    @staticmethod
    def _derive(gold: DataFrame, spec: EnrichmentColumn, values: Series) -> Series:
        """`values` (a coluna `spec.source`) com `map`, `upper` e
        `negative_when` (-|valor| nas linhas da condição) aplicados."""
        if spec.mapping:
            values = values.replace(spec.mapping)
        if spec.upper:
            values = values.str.upper()
        if spec.negative_when:
            condition = spec.negative_when
            mask = gold[condition.column] == condition.equals
            values = values.where(~mask, -values.abs())
        return values

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
