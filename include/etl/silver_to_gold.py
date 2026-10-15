import pandas as pd
import pyarrow as pa
from pandas import DataFrame, Series

from include.etl.bronze_to_silver import file_date

DERIVED_COLUMNS = {"dt_ref", "id_calendario_dt_ref"}


def arrow_schema(columns: list[dict]) -> pa.Schema:
    return pa.schema(
        [(column["name"], pa.type_for_alias(column["type"])) for column in columns]
    )


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


def _convert(values: Series, column: dict) -> Series:
    type_name = column["type"]
    if type_name == "string":
        if column.get("only_digits"):
            values = values.str.replace(r"\D", "", regex=True).replace("", None)
        return values
    if type_name.startswith(("float", "double")):
        return pd.to_numeric(values)
    if type_name.startswith(("int", "uint")):
        return pd.to_numeric(values).astype("Int64")
    if type_name.startswith("timestamp"):
        return _to_datetime(values)
    if type_name.startswith("date"):
        return _to_datetime(values).dt.date
    raise ValueError(f"Tipo '{type_name}' da coluna '{column['name']}' não suportado.")


def to_gold(silver: DataFrame, columns: list[dict], file_name: str) -> DataFrame:
    """Converte a silver (lida com dtype=str) para as colunas e tipos da gold."""
    missing = [
        column["name"]
        for column in columns
        if column["name"] not in DERIVED_COLUMNS
        and column["name"] not in silver.columns
    ]
    if missing:
        raise ValueError(
            f"Colunas do schema da gold ausentes na silver: {missing}. "
            f"Colunas da silver: {list(silver.columns)}"
        )

    dt_ref = file_date(file_name)
    gold = DataFrame(index=silver.index)
    for column in columns:
        name = column["name"]
        try:
            if name == "dt_ref":
                gold[name] = dt_ref
            elif name == "id_calendario_dt_ref":
                gold[name] = int(dt_ref.strftime("%Y%m%d"))
            else:
                gold[name] = _convert(silver[name], column)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Falha ao converter a coluna '{name}' para {column['type']}: {exc}"
            ) from exc
    return gold.reset_index(drop=True)
