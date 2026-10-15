import logging

import pandas as pd
from pandas import DataFrame

from include.config import TIMEZONE
from include.etl.layer import files_to_records, records_to_files, relative_path
from include.etl.log import log_group

logger = logging.getLogger(__name__)


class LayerComparator:
    def between_layers(
        self, source_name: str, source: list[dict], target_name: str, target: list[dict]
    ) -> list[dict]:
        """Arquivos de `source` sem correspondente em `target` (novos) ou
        modificados depois da versão que está em `target`, com o
        `LastModified` do `source`. Casa pelo caminho sem extensão: `target`
        é gravado no mesmo diretório de `source` e, com CNPJ ou partição,
        vários arquivos têm o mesmo `FileName`."""
        source_files, target_files = (
            records_to_files(records).assign(
                Path=lambda df: df["Key"].map(relative_path),
                Label=lambda df: df["Path"],
            )
            for records in (source, target)
        )
        pending = self._pending_files(source_files, target_files, on="Path")
        self._log_pending_files(source_name, target_name, pending)
        return files_to_records(
            pending.rename(columns={"LastModified_x": "LastModified"})
        )

    def by_file_name(
        self, source_name: str, source: list[dict], target_name: str, target: list[dict]
    ) -> list[dict]:
        """Como `between_layers`, mas junta os arquivos de `source` com o mesmo
        `FileName` (CNPJs/partições da mesma data) num registro só: `Keys` são
        as chaves e o `LastModified` é o do arquivo mais recente. Assim uma
        partição que chega depois reprocessa a data com todas."""
        grouped = (
            records_to_files(source)
            .groupby("FileName", as_index=False)
            .agg(
                Keys=("Key", lambda keys: sorted(keys)),
                LastModified=("LastModified", "max"),
            )
        )
        grouped["Label"] = [
            f"{name} ({len(keys)} arquivo(s))"
            for name, keys in zip(grouped["FileName"], grouped["Keys"])
        ]
        pending = self._pending_files(grouped, records_to_files(target), on="FileName")
        self._log_pending_files(source_name, target_name, pending)
        return pending[["FileName", "Keys"]].to_dict("records")

    def with_redshift(self, gold: list[dict], loaded: list[dict]) -> list[dict]:
        """Arquivos da gold a carregar: `insert` (dt_ref ainda não está na
        tabela) ou `update` (arquivo modificado depois da última carga).
        `loaded` são os `{dt_ref, dt_relatorio}` já carregados no Redshift."""
        pending = self._pending_loads(
            records_to_files(gold), self._loaded_to_frame(loaded)
        )
        pending = pending.assign(
            FileName=pending["Key"].str.split("/").str[-1].str.split(".").str[0]
        )
        self._log_pending_loads(pending)
        return pending[["Key", "FileName", "tipo"]].to_dict("records")

    def _pending_files(
        self, source: DataFrame, target: DataFrame, on: str
    ) -> DataFrame:
        merged = pd.merge(source, target[[on, "LastModified"]], on=on, how="left")
        is_new = merged["LastModified_y"].isna()
        is_modified = merged["LastModified_x"] > merged["LastModified_y"]
        return merged[is_new | is_modified]

    def _pending_loads(self, gold: DataFrame, loaded: DataFrame) -> DataFrame:
        if loaded.empty:
            return gold.assign(tipo="insert")[["Key", "tipo"]]

        compare = pd.merge(
            self._gold_by_dt_ref(gold),
            self._loaded_by_dt_ref(loaded),
            left_on="FileName",
            right_on="dt_ref",
            how="left",
        )
        inserts = compare[compare["dt_ref"].isnull()].assign(tipo="insert")
        updates = compare[compare["LastModified"] > compare["dt_relatorio"]].assign(
            tipo="update"
        )
        return pd.concat([inserts, updates])[["Key", "tipo"]]

    def _loaded_to_frame(self, loaded: list[dict]) -> DataFrame:
        """A comparação é feita em horário de Brasília sem fuso; se o
        `dt_relatorio` vier como timestamptz, normaliza igual."""
        frame = pd.DataFrame(loaded, columns=["dt_ref", "dt_relatorio"])
        frame["dt_relatorio"] = pd.to_datetime(frame["dt_relatorio"])
        if frame["dt_relatorio"].dt.tz is not None:
            frame["dt_relatorio"] = (
                frame["dt_relatorio"].dt.tz_convert(TIMEZONE).dt.tz_localize(None)
            )
        return frame

    def _gold_by_dt_ref(self, gold: DataFrame) -> DataFrame:
        """`LastModified` em Brasília sem fuso e `FileName` → dt_ref AAAAMMDD."""
        gold = gold.copy()
        gold["LastModified"] = (
            gold["LastModified"].dt.tz_convert(TIMEZONE).dt.tz_localize(None)
        )
        gold["FileName"] = gold["FileName"].str[-8:].astype("Int64")
        return gold

    def _loaded_by_dt_ref(self, loaded: DataFrame) -> DataFrame:
        """`dt_ref` AAAA-MM-DD → AAAAMMDD, para casar com o nome do arquivo."""
        loaded = loaded.copy()
        loaded["dt_ref"] = (
            loaded["dt_ref"].astype("string").str.replace("-", "").astype("Int64")
        )
        return loaded

    def _log_pending_files(
        self, source_name: str, target_name: str, pending: DataFrame
    ) -> None:
        label = f"{source_name.capitalize()} x {target_name}"
        if pending.empty:
            logger.info(f"{label}: nenhum arquivo novo ou modificado.")
            return

        is_new = pending["LastModified_y"].isna()
        logger.info(
            f"{label}: {len(pending)} arquivo(s) para processar "
            f"({is_new.sum()} novo(s), {(~is_new).sum()} modificado(s))."
        )
        log_group(
            logger,
            f"Arquivos para processar ({len(pending)})",
            [
                f"{row.Label}  "
                + (
                    "novo"
                    if new
                    else f"modificado ({target_name} de {row.LastModified_y:%Y-%m-%d %H:%M} UTC)"
                )
                for row, new in zip(pending.itertuples(), is_new)
            ],
        )

    def _log_pending_loads(self, pending: DataFrame) -> None:
        if pending.empty:
            logger.info("Gold x redshift: nenhum arquivo novo ou modificado.")
            return

        counts = pending["tipo"].value_counts()
        logger.info(
            f"Gold x redshift: {len(pending)} arquivo(s) para carregar "
            f"({counts.get('insert', 0)} insert, {counts.get('update', 0)} update)."
        )
        log_group(
            logger,
            f"Arquivos para carregar ({len(pending)})",
            [f"{row.FileName}  {row.tipo}" for row in pending.itertuples()],
        )
