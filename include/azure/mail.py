import base64
import mimetypes
from dataclasses import dataclass
from typing import Dict, Iterable, Union

import requests

from include.azure.auth import AzureAuth


@dataclass
class Mail:
    auth: AzureAuth | None = None
    sender_email: str = "dw@investsmart.com.br"

    def __post_init__(self):
        if self.auth is None:
            self.auth = AzureAuth()

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.auth.get_token()}",
            "Content-Type": "application/json",
        }

    def _recipients(self, addresses: Union[str, Iterable[str]] | None) -> list:
        if not addresses:
            return []
        if isinstance(addresses, str):
            addresses = [addresses]
        return [{"emailAddress": {"address": address}} for address in addresses]

    def _message_attachments(self, attachments: Dict[str, str] | None) -> list:
        if not attachments:
            return []

        attachment_list = []
        for filename, filepath in attachments.items():
            with open(filepath, "rb") as f:
                file_content = base64.b64encode(f.read()).decode("utf-8")

            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            attachment_list.append(
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": filename,
                    "contentType": mime_type,
                    "contentBytes": file_content,
                }
            )
        return attachment_list

    def send(
        self,
        to: Union[str, Iterable[str]],
        subject: str,
        html: str,
        cc: Union[str, Iterable[str]] | None,
        attachments: Dict[str, str] | None,
    ):
        message = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": html},
                "toRecipients": self._recipients(to),
                "ccRecipients": self._recipients(cc),
            },
            "saveToSentItems": "true",
        }

        if attachments:
            message["message"]["attachments"] = self._message_attachments(attachments)

        response = requests.post(
            f"https://graph.microsoft.com/v1.0/users/{self.sender_email}/sendMail",
            headers=self.headers,
            json=message,
        )
        response.raise_for_status()
        return True


if __name__ == "__main__":
    import os

    import pandas as pd

    TODAY_ID = "20240626"
    file_path = f"ClientesSiglaInválida_{TODAY_ID}.xlsx"
    file_name = f"ClientesSiglaInválida_{TODAY_ID}.xlsx"
    captacao_parcial = pd.DataFrame(
        {
            "Cliente": [12345, 67890],
            "Assessor": ["ABC123", "DEF456"],
            "Sigla": ["SIG1", "SIG2"],
        }
    )
    captacao_parcial.to_excel(rf"{file_path}", index=False)

    file_path2 = f"ClientesSiglaInválida2_{TODAY_ID}.xlsx"
    file_name2 = f"ClientesSiglaInválida2_{TODAY_ID}.xlsx"
    captacao_parcial = pd.DataFrame(
        {
            "Cliente": [12345, 67890],
            "Assessor": ["ABC123", "DEF456"],
            "Sigla": ["SIG1", "SIG2"],
        }
    )
    captacao_parcial.to_excel(rf"{file_path2}", index=False)

    Mail().send(
        to="lucas.gaviao@investsmart.com.br",
        subject="Relatório com cópia",
        html="<p>Email com cópia para o time.</p>",
        cc=["lucas.gaviao@investsmart.com.br"],
        attachments={file_path: file_name, file_path2: file_name2},
    )

    os.remove(file_path)
    os.remove(file_path2)
