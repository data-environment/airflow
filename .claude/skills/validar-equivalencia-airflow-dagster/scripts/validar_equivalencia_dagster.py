"""Compara o código do Dagster (xp) com os contratos YAML do Airflow.

Para cada contrato em dags/xp/contracts, verifica:
  1. Contrato (sem executar nada): colunas (origem, tipo, tamanho, padrão),
     Redshift, S3, checks de qualidade e regras de negócio, comparando
     `contracts.xp.<pipeline>` (Dagster) com o YAML do Airflow.
  2. Transformação (`--sample`): a mesma amostra de silver passa pela
     transformação do Dagster (`dict_transform_silver_to_gold`) e pelo
     `SilverToGold.transform` do Airflow; as saídas são comparadas
     linha a linha, na mesma ordem.

Roda com o venv do xp (tem pandas, pyarrow, pydantic e o pacote `contracts`):
    <dagster>\\xp\\venv\\Scripts\\python.exe <skill>\\scripts\\validar_equivalencia_dagster.py
    <dagster>\\xp\\venv\\Scripts\\python.exe <skill>\\scripts\\validar_equivalencia_dagster.py --pipelines captacao
    <dagster>\\xp\\venv\\Scripts\\python.exe <skill>\\scripts\\validar_equivalencia_dagster.py --sample captacao=amostra\\captacao_20260115.csv

A amostra é uma silver real (colunas com o nome da silver, lida como texto).
O nome do arquivo precisa ter a data AAAAMMDD, como na bronze.

Sai com código 1 se houver divergência.
"""

import argparse
import importlib
import pkgutil
import re
import sys
import types
import warnings
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Union, get_args, get_origin

# scripts/ -> skill -> skills -> .claude -> repo do airflow
ROOT = Path(__file__).resolve().parents[4]
DAGSTER_ROOT = Path(r"C:\Users\Jujubs\Desktop\dagster-orchestration-system")
sys.path[:0] = [str(ROOT), str(DAGSTER_ROOT / "xp"), str(DAGSTER_ROOT)]

try:
    import numpy as np
    import pandas as pd
    from contracts.model import DataContract
except ImportError as exc:
    sys.exit(f"Rode com o venv do xp ({exc}).")

from include.etl.contract import Contract, file_date, load_contracts  # noqa: E402
from include.etl.etl_silver_to_gold import SilverToGold  # noqa: E402

warnings.simplefilter("ignore", FutureWarning)  # pandas, dentro do código do Airflow

CONTRACTS_DIR = ROOT / "dags" / "xp" / "contracts"
TYPE_NAMES = {str: "string", Decimal: "decimal", date: "date", int: "int", float: "float"}


@dataclass
class Finding:
    severity: str  # "DIVERGE" ou "AVISO"
    message: str


@dataclass
class Column:
    name: str
    type: str
    nullable: bool
    min_length: int | None
    max_length: int | None
    pattern: str | None


# --- Lado Dagster -----------------------------------------------------------


def dagster_contract(pipeline: str) -> DataContract | None:
    """Contrato de maior versão em `contracts.xp.<pipeline>`."""
    try:
        package = importlib.import_module(f"contracts.xp.{pipeline}")
    except ModuleNotFoundError:
        return None
    versions = [
        (int(match[1]), mod.name)
        for mod in pkgutil.iter_modules(package.__path__)
        if (match := re.fullmatch(rf"{re.escape(pipeline)}_v(\d+)", mod.name))
    ]
    if not versions:
        return None
    module = importlib.import_module(f"contracts.xp.{pipeline}.{max(versions)[1]}")
    return next(value for value in vars(module).values() if isinstance(value, DataContract))


def dagster_contract_versions(pipeline: str) -> int:
    package = importlib.import_module(f"contracts.xp.{pipeline}")
    return sum(
        1
        for mod in pkgutil.iter_modules(package.__path__)
        if re.fullmatch(rf"{re.escape(pipeline)}_v\d+", mod.name)
    )


def _unwrap_optional(annotation) -> tuple[object, bool]:
    if get_origin(annotation) in (Union, types.UnionType):
        args = get_args(annotation)
        others = [arg for arg in args if arg is not type(None)]
        if len(others) == 1:
            return others[0], type(None) in args
    return annotation, False


def dagster_columns(model) -> list[Column]:
    columns = []
    for name, info in model.model_fields.items():
        annotation, nullable = _unwrap_optional(info.annotation)
        constraints = {}
        for meta in info.metadata:
            for key in ("min_length", "max_length", "pattern"):
                value = getattr(meta, key, None)
                if value is not None:
                    constraints[key] = value
        columns.append(
            Column(
                name=info.alias or name,
                type=TYPE_NAMES.get(annotation, getattr(annotation, "__name__", str(annotation))),
                nullable=nullable,
                min_length=constraints.get("min_length"),
                max_length=constraints.get("max_length"),
                pattern=constraints.get("pattern"),
            )
        )
    return columns


# --- Comparação de contrato -------------------------------------------------


def compare_contract(airflow: Contract, dagster: DataContract) -> list[Finding]:
    findings: list[Finding] = []

    dagster_versions = dagster_contract_versions(airflow.pipeline)
    airflow_versions = len(airflow.bronze_to_silver.versions)
    if dagster_versions != airflow_versions:
        findings.append(
            Finding(
                "AVISO",
                f"versões: Dagster tem {dagster_versions}, YAML tem {airflow_versions} "
                "(comparado só a última versão de cada lado)",
            )
        )

    version = airflow.bronze_to_silver.versions[-1]
    dg_cols = dagster_columns(dagster.schema.model)
    af_cols = version.columns
    dg_by_name = {c.name: c for c in dg_cols}
    af_by_source = {c.source: c for c in af_cols}

    for name in dg_by_name:
        if name not in af_by_source:
            findings.append(Finding("DIVERGE", f"coluna '{name}' existe no Dagster e não no YAML"))
    for source in af_by_source:
        if source not in dg_by_name:
            findings.append(Finding("DIVERGE", f"coluna '{source}' existe no YAML e não no Dagster"))

    for name, dg in dg_by_name.items():
        af = af_by_source.get(name)
        if af is None:
            continue
        af_type = af.type
        if dg.type in ("string", "decimal") and dg.type != af_type:
            findings.append(Finding("DIVERGE", f"'{name}': tipo Dagster={dg.type} YAML={af_type}"))
        elif dg.type not in ("string", "decimal"):
            findings.append(Finding("AVISO", f"'{name}': tipo {dg.type} no Dagster, YAML={af_type}"))
        # `min_length: 0` não restringe nada, igual a não informar.
        if (dg.min_length or None) != (af.min_length or None):
            findings.append(
                Finding("DIVERGE", f"'{name}': min_length Dagster={dg.min_length} YAML={af.min_length}")
            )
        if dg.max_length != af.max_length:
            findings.append(
                Finding("DIVERGE", f"'{name}': max_length Dagster={dg.max_length} YAML={af.max_length}")
            )
        if dg.pattern != af.pattern:
            findings.append(
                Finding("DIVERGE", f"'{name}': pattern Dagster={dg.pattern!r} YAML={af.pattern!r}")
            )
        if dg.nullable == af.required:
            findings.append(
                Finding(
                    "AVISO",
                    f"'{name}': Dagster {'aceita' if dg.nullable else 'não aceita'} nulo, "
                    f"YAML required={af.required}",
                )
            )

    dg_ingestion = dagster.data_ingestion
    dg_redshift = dg_ingestion.redshift_ingestion
    af_redshift = airflow.redshift
    for label, dg_value, af_value in (
        ("database", dg_redshift.redshift_database, af_redshift.database),
        ("schema", dg_redshift.redshift_schema, af_redshift.schema_name),
        ("table", dg_redshift.redshift_table, af_redshift.table),
    ):
        if dg_value != af_value:
            findings.append(Finding("DIVERGE", f"redshift.{label}: Dagster={dg_value} YAML={af_value}"))
    if dg_redshift.redshift_write_mode == "overwrite" and af_redshift.load_mode == "replace_partition":
        findings.append(
            Finding(
                "AVISO",
                "carga: Dagster faz overwrite da tabela; YAML substitui só as partições dt_ref "
                "do arquivo. Resultado só é igual se o arquivo cobrir a tabela inteira",
            )
        )

    dg_s3 = dg_ingestion.s3_ingestion
    # Dagster grava `year=<ano>/` e `<nome>_YYYYMMDD.<ext>`; o YAML só tem o
    # caminho-base e o nome-base (o layer acrescenta o resto).
    dg_path = re.sub(r"/year=.*$", "", dg_s3.s3_path)
    dg_file = re.sub(r"_(YYYYMMDD|\d{8})\..*$", "", dg_s3.s3_file)
    if dg_path != airflow.s3.path:
        findings.append(Finding("DIVERGE", f"s3.path: Dagster={dg_s3.s3_path} YAML={airflow.s3.path}"))
    if dg_file != airflow.s3.file:
        findings.append(Finding("DIVERGE", f"s3.file: Dagster={dg_s3.s3_file} YAML={airflow.s3.file}"))

    for check in dagster.data_quality.checks:
        findings.append(
            Finding("AVISO", f"check Dagster sem equivalente no YAML (revisar): {check.description}")
        )
    for rule in dagster.business_rules.rules:
        findings.append(
            Finding("AVISO", f"regra de negócio Dagster sem check no YAML (revisar): {rule.name}")
        )

    return findings


# --- Transformação ----------------------------------------------------------


def _stub_dagster_secrets() -> None:
    """Os `definitions` do Dagster importam `shared.aws.secret_manager` e
    `shared.azure.mail`, que buscam segredos na AWS ao carregar. Aqui só a
    transformação importa, então os dois viram módulos vazios."""
    for name in ("shared.aws.secret_manager", "shared.azure.mail"):
        if name not in sys.modules:
            stub = types.ModuleType(name)
            stub.__getattr__ = lambda attr: (lambda *args, **kwargs: None)
            sys.modules[name] = stub


def dagster_transform(pipeline: str):
    """Dict `date -> função` de `dict_transform_silver_to_gold` do pipeline."""
    _stub_dagster_secrets()
    module = importlib.import_module(f"pipeline_{pipeline}.definitions")
    for value in vars(module).values():
        transforms = getattr(value, "dict_transform_silver_to_gold", None)
        if transforms is not None:
            return transforms
    raise LookupError(f"pipeline_{pipeline}.definitions não tem dict_transform_silver_to_gold")


def pick_dagster_function(transforms: dict, dt_ref: date):
    eligible = [key for key in transforms if key <= dt_ref]
    if not eligible:
        raise LookupError(f"nenhuma transformação do Dagster vale para {dt_ref}")
    return transforms[max(eligible)]


def _norm(value):
    if value is None or (isinstance(value, float) and np.isnan(value)) or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date().isoformat() if value.time() == datetime.min.time() else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (float, np.floating, Decimal)):
        # `+ 0.0` troca -0.0 por 0.0: o Airflow gera -0 ao negar um zero.
        return f"{float(value) + 0.0:.6f}"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    return str(value)


def compare_frames(dagster_df: pd.DataFrame, airflow_df: pd.DataFrame) -> list[Finding]:
    diffs: list[Finding] = []
    dg_cols, af_cols = list(dagster_df.columns), list(airflow_df.columns)
    if set(dg_cols) != set(af_cols):
        only_dg = [c for c in dg_cols if c not in af_cols]
        only_af = [c for c in af_cols if c not in dg_cols]
        diffs.append(Finding("DIVERGE", f"colunas: só no Dagster {only_dg}, só no Airflow {only_af}"))
    elif dg_cols != af_cols:
        # Ordem só muda o COPY sem lista de colunas; os valores continuam iguais.
        diffs.append(Finding("AVISO", f"ordem das colunas: Dagster {dg_cols} x Airflow {af_cols}"))
    if len(dagster_df) != len(airflow_df):
        diffs.append(Finding("DIVERGE", f"linhas: Dagster {len(dagster_df)} x Airflow {len(airflow_df)}"))
        return diffs

    for column in dg_cols:
        if column not in af_cols:
            continue
        left = dagster_df[column].map(_norm).tolist()
        right = airflow_df[column].map(_norm).tolist()
        mismatches = [i for i, (a, b) in enumerate(zip(left, right)) if a != b]
        if mismatches:
            examples = "; ".join(f"linha {i}: {left[i]!r} x {right[i]!r}" for i in mismatches[:3])
            diffs.append(
                Finding("DIVERGE", f"'{column}': {len(mismatches)} valor(es) diferente(s) ({examples})")
            )
    return diffs


def compare_transform(airflow: Contract, pipeline: str, sample_path: Path) -> list[Finding]:
    dt_ref = file_date(sample_path.name)
    sample = pd.read_csv(sample_path, sep=None, engine="python", dtype=str)

    dagster_fn = pick_dagster_function(dagster_transform(pipeline), dt_ref)
    dagster_out = dagster_fn(sample.copy(), dt_ref)

    # `layer` falso: o `transform` não usa o Layer (e `contract.layer()` importa include.config, que lê o Secrets Manager).
    airflow_out = SilverToGold(airflow, layer=object()).transform(sample.copy(), sample_path.stem)

    return compare_frames(dagster_out, airflow_out)


# --- Execução ---------------------------------------------------------------


def report(pipeline: str, findings: list[Finding], transform: list[Finding] | None, error: str | None) -> bool:
    divergent = any(f.severity == "DIVERGE" for f in findings + (transform or [])) or bool(error)
    print(f"\n=== {pipeline}: {'DIVERGENTE' if divergent else 'OK'}")
    for finding in findings:
        print(f"  [{finding.severity}] {finding.message}")
    if transform is None:
        print("  [transformação] não executada (sem --sample)")
    elif not transform:
        print("  [transformação] saídas iguais")
    for finding in transform or []:
        print(f"  [{finding.severity} transformação] {finding.message}")
    if error:
        print(f"  [ERRO] {error}")
    return not divergent


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pipelines", nargs="+", help="pipelines a validar (padrão: todos os contratos)")
    parser.add_argument("--contracts-dir", type=Path, default=CONTRACTS_DIR, help="pasta dos YAMLs do Airflow")
    parser.add_argument(
        "--sample",
        action="append",
        default=[],
        metavar="PIPELINE=ARQUIVO",
        help="amostra de silver para a comparação de transformação (pode repetir)",
    )
    args = parser.parse_args()

    samples = {}
    for item in args.sample:
        pipeline, _, path = item.partition("=")
        samples[pipeline] = Path(path)

    contracts = load_contracts(args.contracts_dir)
    if args.pipelines:
        contracts = [c for c in contracts if c.pipeline in args.pipelines]
    if not contracts:
        sys.exit("Nenhum contrato encontrado.")

    results = {}
    for contract in contracts:
        pipeline = contract.pipeline
        findings: list[Finding] = []
        transform: list[Finding] | None = None
        error = None
        try:
            dagster = dagster_contract(pipeline)
            if dagster is None:
                findings.append(Finding("DIVERGE", f"sem contrato em contracts.xp.{pipeline}"))
            else:
                findings = compare_contract(contract, dagster)
        except Exception as exc:  # noqa: BLE001
            error = f"contrato: {type(exc).__name__}: {exc}"

        if pipeline in samples and not error:
            try:
                transform = compare_transform(contract, pipeline, samples[pipeline])
            except Exception as exc:  # noqa: BLE001
                error = f"transformação: {type(exc).__name__}: {exc}"

        results[pipeline] = report(pipeline, findings, transform, error)

    divergent = [p for p, ok in results.items() if not ok]
    print(f"\nResumo: {len(results) - len(divergent)}/{len(results)} contratos sem divergência.")
    if divergent:
        print(f"Divergentes: {', '.join(divergent)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
