import requests


def send(webhook_url: str, text: str) -> None:
    response = requests.post(webhook_url, json={"text": text}, timeout=30)
    response.raise_for_status()


def failure_message(dag_id: str, task_id: str, exception: BaseException | None) -> str:
    return (
        f":red_circle: *Erro na DAG `{dag_id}`*\n"
        f"*Task:* `{task_id}`\n"
        f"*Tipo:* `{type(exception).__name__ if exception else 'desconhecido'}`\n"
    )
