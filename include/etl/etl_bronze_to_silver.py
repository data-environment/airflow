import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated

import pandas as pd
from pandas import DataFrame
from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    ValidationError,
    create_model,
)

from include.etl.contract import ColumnSpec, Contract, ContractVersion

if TYPE_CHECKING:
    from include.etl.layer import Layer

logger = logging.getLogger(__name__)


class BronzeToSilver:
    NULL_MARKERS = {"", "nan", "none", "null", "''"}
    MAX_ERRORS = 10

    def __init__(self, contract: Contract, layer: "Layer | None" = None):
        self.contract = contract
        self.layer = layer or contract.layer()

    def run(self, file_name: str, bronze_key: str) -> dict:
        from include.etl.file_types import build_file_type

        settings, s3 = self.layer.settings, self.layer.s3_client

        version = self.contract.bronze_to_silver.version_for(file_name)
        logger.info(
            f"{file_name}: contrato a partir de {version.valid_from} "
            f"({len(version.columns)} colunas, origem {version.origin.type})"
        )

        reader = build_file_type(version.origin, s3)
        bronze = reader.get(settings.bucket_bronze, bronze_key)
        silver = self.transform(bronze, version)

        writer = build_file_type(version.destination, s3)
        prefix = bronze_key.rsplit("/", 1)[0]
        writer.put(silver, settings.bucket_silver, prefix, file_name)
        silver_key = f"{prefix}/{file_name}.{writer.extension}"

        logger.info(
            f"{file_name}: {len(bronze)} linha(s) lida(s) da bronze, "
            f"{len(silver)} gravada(s) em {settings.bucket_silver}/{silver_key}"
        )
        return {"FileName": file_name, "Key": silver_key, "rows": len(silver)}

    def transform(self, bronze: DataFrame, version: ContractVersion) -> DataFrame:
        columns = version.columns
        df = self._drop_empty_and_filtered(bronze, version)
        self._check_required_columns(df, columns)

        model = self._build_model(columns)
        rows, errors = [], []
        for index, record in zip(df.index, df.to_dict("records")):
            values = {
                column.name: (
                    self._normalize_decimal
                    if column.type == "decimal"
                    else self._normalize_text
                )(record.get(column.source))
                for column in columns
            }
            try:
                rows.append(model(**values).model_dump())
            except ValidationError as exc:
                errors.extend(
                    f"linha {index + 2}, {error['loc'][0]}: {error['msg']} "
                    f"(valor: {str(error.get('input'))[:50]!r})"
                    for error in exc.errors()
                )

        if errors:
            more = (
                f"\n... e mais {len(errors) - self.MAX_ERRORS}"
                if len(errors) > self.MAX_ERRORS
                else ""
            )
            raise ValueError(
                f"{len(errors)} erro(s) de validação do contrato:\n"
                + "\n".join(errors[: self.MAX_ERRORS])
                + more
            )

        return pd.DataFrame(rows, columns=[column.name for column in columns])

    def _drop_empty_and_filtered(
        self, bronze: DataFrame, version: ContractVersion
    ) -> DataFrame:
        df = bronze.dropna(how="all")
        for rule in version.filter_rules:
            if rule.column not in df.columns:
                continue
            values = df[rule.column].astype(str)
            if rule.op == "startswith":
                match = values.str.startswith(rule.value)
            elif rule.op == "equals":
                match = values == rule.value
            else:
                match = values.str.contains(rule.value, regex=rule.op == "regex")
            df = df[~match]
        if df.empty:
            raise ValueError(
                "O arquivo está vazio (depois de remover linhas filtradas)."
            )
        return df

    def _check_required_columns(self, df: DataFrame, columns: list[ColumnSpec]) -> None:
        missing = [
            column.source
            for column in columns
            if column.required and column.source not in df.columns
        ]
        if missing:
            raise ValueError(
                f"Faltam colunas obrigatórias no arquivo: {missing}. "
                f"Colunas encontradas: {list(df.columns)}"
            )

    @staticmethod
    def _build_model(columns: list[ColumnSpec]) -> type[BaseModel]:
        fields = {}
        for column in columns:
            if column.type == "decimal":
                annotation = Decimal
            else:
                annotation = Annotated[
                    str,
                    StringConstraints(
                        min_length=column.min_length,
                        max_length=column.max_length,
                        pattern=column.pattern,
                    ),
                ]
            if column.required:
                fields[column.name] = (annotation, ...)
            else:
                fields[column.name] = annotation | None
        return create_model(
            "SilverRow", __config__=ConfigDict(extra="ignore"), **fields
        )

    @classmethod
    def _normalize_text(cls, value) -> str | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        return None if text.lower() in cls.NULL_MARKERS else text

    @classmethod
    def _normalize_decimal(cls, value) -> str | None:
        """Aceita formato BR (1.234,56) e US (1,234.56 / 1234.56)."""
        text = cls._normalize_text(value)
        if text is None:
            return None
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                return text.replace(".", "").replace(",", ".")
            return text.replace(",", "")
        return text.replace(",", ".")
