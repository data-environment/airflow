import logging
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from include.etl.contract import Contract, HistoryRangeCheck, VolumeCheck

if TYPE_CHECKING:
    from include.aws.redshift.client import RedshiftClient

logger = logging.getLogger(__name__)


class QualityChecker:
    """Checagens do contrato (`quality`) contra o histórico já carregado no
    Redshift. Rodam com o arquivo na staging, antes do DELETE+INSERT: se
    alguma falhar, a tabela final não é tocada."""

    def __init__(self, contract: Contract, client: "RedshiftClient"):
        self.checks = contract.quality
        self.table = contract.redshift.qualified_table
        self.schema = contract.redshift.schema_name
        self.client = client

    def run(self, staging_table: str, dt_ref: date, rows: int) -> None:
        failures = []
        for check in self.checks:
            if isinstance(check, VolumeCheck):
                failure = self._volume(check, dt_ref, rows)
            else:
                failure = self._history_range(check, staging_table, dt_ref)
            if failure:
                failures.append(failure)
        if failures:
            raise ValueError(
                f"{len(failures)} checagem(ns) de qualidade falharam para dt_ref "
                f"{dt_ref} (nada foi carregado):\n" + "\n".join(failures)
            )

    def _volume(self, check: VolumeCheck, dt_ref: date, rows: int) -> str | None:
        history = self.client.get(
            f"SELECT dt_ref, COUNT(*) AS linhas FROM {self.table} "
            f"WHERE dt_ref < '{dt_ref}' GROUP BY dt_ref "
            f"ORDER BY dt_ref DESC LIMIT {check.last}"
        )
        if history.empty:
            logger.warning("volume: sem histórico no Redshift, checagem ignorada.")
            return None

        limit = float(history["linhas"].mean()) * check.max_ratio
        message = (
            f"volume: {rows} linha(s); limite {limit:,.0f} = média das últimas "
            f"{len(history)} dt_ref ({history['linhas'].mean():,.0f}) "
            f"× {check.max_ratio}"
        )
        return self._result(message, rows <= limit)

    def _history_range(
        self, check: HistoryRangeCheck, staging_table: str, dt_ref: date
    ) -> str | None:
        current = self._scalar(
            f"SELECT {check.metric} AS valor FROM {self.schema}.{staging_table}"
        )
        start = (pd.Timestamp(dt_ref) - pd.DateOffset(months=check.months + 1)).replace(
            day=1
        )
        history = self.client.get(
            f"SELECT dt_ref, {check.metric} AS valor FROM {self.table} "
            f"WHERE dt_ref < '{dt_ref}' AND dt_ref >= '{start.date()}' "
            f"GROUP BY dt_ref"
        )
        closings = self.monthly_closings(history, dt_ref, check.months)
        if len(closings) < 2:
            logger.warning(
                f"{check.name}: menos de 2 fechamentos mensais no Redshift, "
                "checagem ignorada."
            )
            return None

        base = float(closings.iloc[-1])
        variation = self.max_monthly_variation(closings)
        low = base * (1 - variation * check.multiplier)
        high = base * (1 + variation * check.multiplier)
        message = (
            f"{check.name}: {current:,.2f}; faixa [{low:,.2f}, {high:,.2f}] = "
            f"fechamento de {closings.index[-1]} ({base:,.2f}) × "
            f"(1 ± {variation:.2%} × {check.multiplier})"
        )
        return self._result(message, low <= current <= high)

    @staticmethod
    def monthly_closings(history: pd.DataFrame, dt_ref: date, months: int) -> pd.Series:
        """Valor da última dt_ref de cada mês anterior ao de `dt_ref`, do mais
        antigo ao mais recente (até `months + 1` meses: dá `months` variações)."""
        if history.empty:
            return pd.Series(dtype=float)
        frame = history.assign(
            dt_ref=pd.to_datetime(history["dt_ref"]),
            valor=pd.to_numeric(history["valor"]).astype(float),
        )
        frame = frame[frame["dt_ref"] < pd.Timestamp(dt_ref).replace(day=1)]
        closings = (
            frame.sort_values("dt_ref")
            .groupby(frame["dt_ref"].dt.to_period("M"))["valor"]
            .last()
        )
        return closings.iloc[-(months + 1) :]

    @staticmethod
    def max_monthly_variation(closings: pd.Series) -> float:
        """Maior |c_m / c_(m-1) − 1| entre fechamentos seguidos."""
        previous, current = closings.iloc[:-1].to_numpy(), closings.iloc[1:].to_numpy()
        valid = previous != 0
        if not valid.any():
            return 0.0
        return float(abs(current[valid] / previous[valid] - 1).max())

    def _scalar(self, query: str) -> float:
        value = self.client.get(query).iloc[0, 0]
        return 0.0 if pd.isna(value) else float(value)

    @staticmethod
    def _result(message: str, passed: bool) -> str | None:
        if passed:
            logger.info(f"OK {message}")
            return None
        logger.error(f"FALHOU {message}")
        return message
