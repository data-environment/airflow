---
name: validar-equivalencia-airflow-dagster
description: Verifica se os pipelines XP no Airflow seguem o Dagster, em código (contrato e transformação) e em resultado (contagens no Redshift dev x prod). Use quando o usuário pedir para validar, comparar ou conferir a migração Dagster -> Airflow de um pipeline ou de todos.
---

# Validar equivalência Airflow x Dagster

Duas checagens que se complementam:

1. **Código** — `scripts/validar_equivalencia_dagster.py` (nesta pasta da skill): compara o contrato Dagster (`contracts.xp.<pipeline>`) com o YAML do Airflow e, com `--sample`, executa a transformação dos dois lados sobre a mesma silver.
2. **Resultado** — `scripts/compare_dev_prod.py`: compara as tabelas do Redshift dev (Airflow) com o prod (Dagster): colunas, tipos, linhas totais e linhas por `dt_ref`.

Código igual não garante dado igual (a carga pode divergir), e dado igual hoje não garante código igual (um YAML pode mudar amanhã). Rode as duas quando o objetivo for "está de acordo com o que era".

## Entradas

- Pipelines (opcional): nomes dos contratos em `dags/xp/contracts/*.yaml`. Sem isso, roda tudo.
- Amostra de silver (opcional, só para a checagem de código): um arquivo `<nome>_AAAAMMDD.csv` por pipeline.
- Data de corte (opcional, só para a checagem de resultado): `--since AAAA-MM-DD`.

## Passo 1 — checagem de código

Use o Python do venv do xp, que tem pandas, pyarrow, pydantic e o pacote `contracts`:

```
c:\Users\Jujubs\Desktop\dagster-orchestration-system\xp\venv\Scripts\python.exe .claude\skills\validar-equivalencia-airflow-dagster\scripts\validar_equivalencia_dagster.py --pipelines <p1> <p2>
```

Para incluir a transformação:

```
... --pipelines <p> --sample <p>=<caminho>\<p>_AAAAMMDD.csv
```

Sem `--sample`, só o contrato é comparado. Diga isso no relatório.

## Passo 2 — checagem de resultado

Use um Python com `boto3`, `pyyaml`, `python-dotenv`, `sqlalchemy` e o driver do Redshift:

```
python .claude\skills\validar-equivalencia-airflow-dagster\scripts\compare_dev_prod.py --tables <p1> <p2> --since AAAA-MM-DD
```

Antes de rodar, confirme que o interpretador tem essas dependências (`python -c "import sqlalchemy, boto3, dotenv"`). Se faltar, avise o usuário e não instale nada sem perguntar.

O script lê credenciais de dois lugares: `.env` deste repo (+ Secrets Manager) para o dev, e o `.env` do `dagster-orchestration-system` para o prod. Não grava nada no banco; só faz `SELECT`.

## Como interpretar

- **DIVERGE** (código): o pipeline não faz a mesma coisa que o Dagster. Corrija o YAML ou a transformação antes de dar o pipeline como migrado.
- **AVISO** (código): diferença de tipo, obrigatoriedade, ordem de colunas, regra de negócio do Dagster sem check no YAML, ou modo de carga diferente. Nem todo aviso é erro; liste para o usuário decidir.
- **Divergência de linhas** (resultado): costuma ser defasagem de carga, não erro. Confira se a partição divergente é a mais recente antes de concluir algo.
- **Colunas só num lado** (resultado): erro de schema. Quase sempre é bloqueante.
- Diferença de `dt_ref` antigo que o Dagster já reprocessou não é erro do Airflow. Veja se a data é posterior à migração.

## Relatório

Entregue, por pipeline: `OK`, `DIVERGENTE` ou `NÃO EXECUTADO` (com o motivo), e a lista de DIVERGE e AVISO em texto simples. Separe o que é código do que é resultado. Não diga "migrado" sem ter rodado as duas checagens. Se uma delas não rodou, diga qual e por quê.

## Regras

- Não altere os YAMLs, o Dagster nem o banco durante a validação. Se achar divergência, reporte e pergunte antes de corrigir.
- O `compare_dev_prod.py` consulta o prod. Só faz `SELECT`, mas confirme que o usuário pediu a comparação com prod antes de rodar.
- Os scripts estão sem commit; não faça commit sem pedido.
- Rode os comandos a partir da raiz do repo `airflow`, para que os caminhos relativos funcionem.
