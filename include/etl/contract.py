"""Contrato bronze → silver definido no YAML (`bronze_to_silver`).

Regras portadas do validador SmartCheck (validadordeproducoes):
- a versão do contrato é escolhida pela data YYYYMMDD do nome do arquivo;
- remove linhas vazias e o rodapé do export XP ("Filtros aplicados...");
- renomeia `source` → `name` e valida cada linha com pydantic;
- qualquer linha inválida falha o arquivo inteiro (nada é descartado em silêncio).
"""

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Optional

import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel, ConfigDict, StringConstraints, ValidationError, create_model

FOOTER_MARKER = "Filtros aplicados"
NULL_MARKERS = {"", "nan", "none", "null", "''"}
MAX_ERRORS = 10


def file_date(file_name: str) -> date:
    match = re.search(r"(\d{8})", file_name)
    if not match:
        raise ValueError(f"Arquivo '{file_name}' sem data YYYYMMDD no nome.")
    return datetime.strptime(match.group(1), "%Y%m%d").date()


def select_version(versions: list[dict], file_name: str) -> dict:
    """Versão com o maior `from` que seja <= data do arquivo."""
    dt = file_date(file_name)
    eligible = [version for version in versions if version["from"] <= dt]
    if not eligible:
        raise ValueError(
            f"Nenhuma versão do contrato vale para {file_name} ({dt}); "
            f"versões a partir de: {sorted(v['from'] for v in versions)}."
        )
    return max(eligible, key=lambda version: version["from"])


def _normalize_text(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return None if text.lower() in NULL_MARKERS else text


def _normalize_decimal(value) -> str | None:
    """Aceita formato BR (1.234,56) e US (1,234.56 / 1234.56)."""
    text = _normalize_text(value)
    if text is None:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            return text.replace(".", "").replace(",", ".")
        return text.replace(",", "")
    return text.replace(",", ".")


def build_model(columns: list[dict]) -> type[BaseModel]:
    fields = {}
    for column in columns:
        if column.get("type") == "decimal":
            annotation = Decimal
        else:
            annotation = Annotated[
                str,
                StringConstraints(
                    min_length=column.get("min_length"),
                    max_length=column.get("max_length"),
                    pattern=column.get("pattern"),
                ),
            ]
        if column.get("required", True):
            fields[column["name"]] = (annotation, ...)
        else:
            fields[column["name"]] = (Optional[annotation], None)
    return create_model("SilverRow", __config__=ConfigDict(extra="ignore"), **fields)


def apply_contract(
    bronze: DataFrame, version: dict, footer_column: str | None = None
) -> DataFrame:
    """Aplica uma versão do contrato ao DataFrame lido da bronze (dtype=str)."""
    columns = version["columns"]

    df = bronze.dropna(how="all")
    if footer_column and footer_column in df.columns:
        df = df[~df[footer_column].astype(str).str.startswith(FOOTER_MARKER)]
    if df.empty:
        raise ValueError("O arquivo está vazio (depois de remover linhas vazias e rodapé).")

    missing = [
        column["source"]
        for column in columns
        if column.get("required", True) and column["source"] not in df.columns
    ]
    if missing:
        raise ValueError(
            f"Faltam colunas obrigatórias no arquivo: {missing}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    model = build_model(columns)
    rows, errors = [], []
    for index, record in zip(df.index, df.to_dict("records")):
        values = {
            column["name"]: (
                _normalize_decimal if column.get("type") == "decimal" else _normalize_text
            )(record.get(column["source"]))
            for column in columns
        }
        try:
            rows.append(model(**values).model_dump())
        except ValidationError as exc:
            # Linha do Excel: índice 0 + cabeçalho na linha 1.
            errors.extend(
                f"linha {index + 2}, {error['loc'][0]}: {error['msg']} "
                f"(valor: {str(error.get('input'))[:50]!r})"
                for error in exc.errors()
            )

    if errors:
        more = f"\n... e mais {len(errors) - MAX_ERRORS}" if len(errors) > MAX_ERRORS else ""
        raise ValueError(
            f"{len(errors)} erro(s) de validação do contrato:\n"
            + "\n".join(errors[:MAX_ERRORS])
            + more
        )

    return pd.DataFrame(rows, columns=[column["name"] for column in columns])
