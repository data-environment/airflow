from dataclasses import dataclass, field
from typing import List, Optional

import msal

from include.config import settings


@dataclass
class AzureAuth:
    client_id: str = settings.azure_client_id
    tenant_id: str = settings.azure_tenant_id
    client_secret: str = settings.azure_client_secret
    scopes: List[str] = field(
        default_factory=lambda: ["https://graph.microsoft.com/.default"]
    )

    _app: Optional[msal.ConfidentialClientApplication] = field(
        default=None, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

    @property
    def authority_url(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}"

    def get_token(self) -> str:
        result = self._app.acquire_token_for_client(scopes=self.scopes)

        if "access_token" not in result:
            error_description = result.get("error_description", "Erro desconhecido")
            error_code = result.get("error", "unknown_error")
            raise RuntimeError(
                f"Falha na autenticação Azure AD: [{error_code}] {error_description}"
            )

        return result["access_token"]


if __name__ == "__main__":

    auth = AzureAuth(
        client_id=settings.azure_client_id,
        tenant_id=settings.azure_tenant_id,
        client_secret=settings.azure_client_secret,
    )

    token = auth.get_token()
    print(f"Token obtido: {token}")
