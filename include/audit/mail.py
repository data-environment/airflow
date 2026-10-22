import logging

from include.azure.mail import Mail
from include.config import settings

logger = logging.getLogger(__name__)


def send(to: list[str], cc: list[str], subject: str, html: str) -> None:
    """Envia o email; fora de prod, vai só para `DEV_RECIPIENT` (.env)."""
    if settings.environment != "prod":
        if not settings.dev_recipient:
            logger.warning(
                f"Ambiente {settings.environment} sem DEV_RECIPIENT: "
                f"email '{subject}' para {to} (cc={cc}) não enviado."
            )
            return
        logger.info(
            f"Ambiente {settings.environment}: email para {to} (cc={cc}) "
            f"redirecionado para {settings.dev_recipient}"
        )
        subject = f"[{settings.environment}] {subject}"
        to, cc = [settings.dev_recipient], []

    Mail().send(to=to, subject=subject, html=html, cc=cc, attachments=None)
    logger.info(f"Email '{subject}' enviado para {to} (cc={cc}).")
