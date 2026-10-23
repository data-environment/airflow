"""Compara as tabelas dos contratos entre o Redshift dev e o prod.

Para cada tabela (`redshift.schema`.`redshift.table` dos contratos em
dags/xp/contracts), verifica:
  - se existe nos dois ambientes;
  - diferenças de colunas (nome e tipo);
  - contagem total de linhas;
  - contagem de linhas por partição (`redshift.partition_key`), listando
    só as partições que divergem.

Credenciais:
  - dev:  .env deste repo + Secrets Manager (SECRET_NAME), mesma regra do
          include/aws/secret_manager.py (SM primeiro, .env completa o resto).
  - prod: .env do dagster-orchestration-system.

Só faz SELECT.

Uso:
    python <skill>\\scripts\\compare_dev_prod.py
    python <skill>\\scripts\\compare_dev_prod.py --tables captacao positivador
    python <skill>\\scripts\\compare_dev_prod.py --since 2026-01-01
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import yaml
from dotenv import dotenv_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# scripts/ -> skill -> skills -> .claude -> repo do airflow
ROOT = Path(__file__).resolve().parents[4]
CONTRACTS_DIR = ROOT / "dags" / "xp" / "contracts"
DEV_ENV = ROOT / ".env"
PROD_ENV = Path(r"C:\Users\Jujubs\Desktop\dagster-orchestration-system\.env")


@dataclass
class Table:
    pipeline: str
    database: str
    schema: str
    table: str
    partition_key: str | None

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"


def read_env(path: Path) -> dict[str, str]:
    return {k.strip(): (v or "").strip() for k, v in dotenv_values(path).items()}


def dev_credentials() -> dict[str, str]:
    env = read_env(DEV_ENV)
    secrets: dict[str, str] = {}
    try:
        client = boto3.client(
            "secretsmanager",
            region_name=env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        )
        response = client.get_secret_value(SecretId=env["SECRET_NAME"])
        secrets = {k: str(v) for k, v in json.loads(response["SecretString"]).items()}
    except Exception as exc:
        print(f"[aviso] Secrets Manager indisponível, usando só o .env: {exc}")
    merged = {**env, **secrets}
    return {
        "user": merged.get("REDSHIFT_USER"),
        "password": merged.get("REDSHIFT_PASSWORD"),
        "host": merged.get("REDSHIFT_HOST"),
        "port": merged.get("REDSHIFT_PORT") or "5439",
    }


def prod_credentials() -> dict[str, str]:
    env = read_env(PROD_ENV)
    return {
        "user": env.get("REDSHIFT_USER"),
        "password": env.get("REDSHIFT_PASS"),
        "host": env.get("REDSHIFT_HOST"),
        "port": env.get("REDSHIFT_PORT") or "5439",
    }


def make_engine(name: str, creds: dict[str, str], database: str) -> Engine:
    missing = [k for k, v in creds.items() if not v]
    if missing:
        sys.exit(f"Credenciais de {name} incompletas, faltando: {missing}")
    return create_engine(
        "redshift+psycopg2://{user}:{password}@{host}:{port}/".format(**creds) + database,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 30, "sslmode": "require"},
    )


def load_tables(only: list[str] | None) -> list[Table]:
    tables = []
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        redshift = contract.get("redshift")
        if not redshift:
            continue
        pipeline = contract.get("pipeline", path.stem)
        if only and pipeline not in only and redshift["table"] not in only:
            continue
        tables.append(
            Table(
                pipeline=pipeline,
                database=redshift["database"],
                schema=redshift["schema"],
                table=redshift["table"],
                partition_key=redshift.get("partition_key"),
            )
        )
    return tables


def get_columns(engine: Engine, table: Table) -> dict[str, str] | None:
    query = text(
        """
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": table.schema, "table": table.table}).all()
    if not rows:
        return None
    columns = {}
    for name, data_type, char_len, precision, scale in rows:
        if char_len:
            data_type = f"{data_type}({char_len})"
        elif data_type == "numeric" and precision:
            data_type = f"numeric({precision},{scale})"
        columns[name] = data_type
    return columns


def count_rows(engine: Engine, table: Table, since: str | None) -> int:
    where = f" WHERE {table.partition_key} >= :since" if since and table.partition_key else ""
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table.fqn}{where}"), {"since": since}).scalar()


def count_by_partition(engine: Engine, table: Table, since: str | None) -> dict[str, int]:
    key = table.partition_key
    where = f" WHERE {key} >= :since" if since else ""
    query = text(f"SELECT {key}, COUNT(*) FROM {table.fqn}{where} GROUP BY 1")
    with engine.connect() as conn:
        return {str(k): n for k, n in conn.execute(query, {"since": since}).all()}


def compare(table: Table, dev: Engine, prod: Engine, since: str | None, max_partitions: int) -> bool:
    print(f"\n=== {table.pipeline} ({table.database}.{table.fqn})")

    dev_cols, prod_cols = get_columns(dev, table), get_columns(prod, table)
    if dev_cols is None or prod_cols is None:
        print(f"  existe -> dev: {dev_cols is not None} | prod: {prod_cols is not None}")
        return False

    ok = True
    only_dev = [c for c in dev_cols if c not in prod_cols]
    only_prod = [c for c in prod_cols if c not in dev_cols]
    type_diff = [
        (c, dev_cols[c], prod_cols[c])
        for c in dev_cols
        if c in prod_cols and dev_cols[c] != prod_cols[c]
    ]
    if only_dev:
        ok = False
        print(f"  colunas só no dev:  {only_dev}")
    if only_prod:
        ok = False
        print(f"  colunas só no prod: {only_prod}")
    for col, dev_type, prod_type in type_diff:
        ok = False
        print(f"  tipo diferente: {col}  dev={dev_type}  prod={prod_type}")
    if list(dev_cols) != list(prod_cols) and not (only_dev or only_prod):
        print("  (mesmas colunas, ordem diferente)")

    dev_total, prod_total = count_rows(dev, table, since), count_rows(prod, table, since)
    diff = dev_total - prod_total
    print(f"  linhas -> dev: {dev_total:,} | prod: {prod_total:,} | diferença: {diff:+,}")
    if diff:
        ok = False

    if table.partition_key and table.partition_key in dev_cols and table.partition_key in prod_cols:
        dev_parts = count_by_partition(dev, table, since)
        prod_parts = count_by_partition(prod, table, since)
        divergent = sorted(
            (p for p in dev_parts.keys() | prod_parts.keys()
             if dev_parts.get(p, 0) != prod_parts.get(p, 0)),
            reverse=True,
        )
        if divergent:
            ok = False
            print(f"  {len(divergent)} partição(ões) de {table.partition_key} divergentes"
                  f" (mostrando até {max_partitions}, mais recentes primeiro):")
            for p in divergent[:max_partitions]:
                print(f"    {p}: dev={dev_parts.get(p, 0):,}  prod={prod_parts.get(p, 0):,}")

    if ok:
        print("  OK")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tables", nargs="+", help="pipelines ou tabelas a comparar (padrão: todas)")
    parser.add_argument("--since", help="só considera partições >= esta data (YYYY-MM-DD)")
    parser.add_argument("--max-partitions", type=int, default=20, help="máximo de partições divergentes exibidas")
    args = parser.parse_args()

    tables = load_tables(args.tables)
    if not tables:
        sys.exit("Nenhuma tabela encontrada nos contratos.")

    dev_creds, prod_creds = dev_credentials(), prod_credentials()
    if dev_creds["host"] == prod_creds["host"]:
        print(f"[aviso] dev e prod apontam para o mesmo host: {dev_creds['host']}")

    engines: dict[tuple[str, str], Engine] = {}
    results = {}
    for table in tables:
        for name, creds in (("dev", dev_creds), ("prod", prod_creds)):
            if (name, table.database) not in engines:
                engines[(name, table.database)] = make_engine(name, creds, table.database)
        try:
            results[table.pipeline] = compare(
                table,
                engines[("dev", table.database)],
                engines[("prod", table.database)],
                args.since,
                args.max_partitions,
            )
        except Exception as exc:
            print(f"  ERRO: {exc}")
            results[table.pipeline] = False

    divergent = [p for p, ok in results.items() if not ok]
    print(f"\nResumo: {len(results) - len(divergent)}/{len(results)} tabelas iguais.")
    if divergent:
        print(f"Divergentes: {', '.join(divergent)}")


if __name__ == "__main__":
    main()
