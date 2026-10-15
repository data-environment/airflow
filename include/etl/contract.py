import re
from datetime import date, datetime
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

if TYPE_CHECKING:
    from include.etl.layer import Layer


def file_date(file_name: str) -> date:
    """Data de referência do arquivo: o `AAAAMMDD` no nome."""
    match = re.search(r"(\d{8})", file_name)
    if not match:
        raise ValueError(f"Arquivo '{file_name}' sem data YYYYMMDD no nome.")
    return datetime.strptime(match.group(1), "%Y%m%d").date()


class _Spec(BaseModel):
    # Chave desconhecida no YAML é erro de digitação, não opção nova.
    model_config = ConfigDict(extra="forbid", frozen=True)


class S3Spec(_Spec):
    path: str
    file: str


class FileSpec(BaseModel):
    """Formato de leitura/escrita. Além de `type`, aceita as opções do
    formato (ex.: `sep`, `encoding`, `header`), repassadas ao construtor."""

    model_config = ConfigDict(extra="allow", frozen=True)

    type: Literal["parquet", "csv", "xlsx", "html", "xls"]

    @property
    def options(self) -> dict:
        return self.model_dump(exclude={"type"})


class ColumnSpec(_Spec):
    """Coluna da silver: `source` é o cabeçalho no arquivo da bronze."""

    name: str
    source: str
    type: Literal["string", "decimal"] = "string"
    required: bool = True
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None


class FilterRule(_Spec):
    """Descarta as linhas em que `column` (cabeçalho na bronze) casa com
    `value` segundo `op`."""

    column: str
    op: Literal["startswith", "equals", "contains", "regex"] = "startswith"
    value: str


class ContractVersion(_Spec):
    valid_from: date = Field(alias="from")
    origin: FileSpec
    destination: FileSpec
    filter_rules: list[FilterRule] = []
    columns: list[ColumnSpec] = Field(min_length=1)


class BronzeToSilverSpec(_Spec):
    versions: list[ContractVersion] = Field(min_length=1)

    def version_for(self, file_name: str) -> ContractVersion:
        """Versão com o maior `from` que seja <= data do arquivo."""
        dt = file_date(file_name)
        eligible = [version for version in self.versions if version.valid_from <= dt]
        if not eligible:
            raise ValueError(
                f"Nenhuma versão do contrato vale para {file_name} ({dt}); "
                f"versões a partir de: {sorted(v.valid_from for v in self.versions)}."
            )
        return max(eligible, key=lambda version: version.valid_from)


class GoldColumn(_Spec):
    """Coluna da gold: `type` é um alias de tipo do Arrow (ex.: `float64`)."""

    name: str
    type: str
    only_digits: bool = False

    @field_validator("type")
    @classmethod
    def _arrow_alias(cls, value: str) -> str:
        import pyarrow as pa

        try:
            pa.type_for_alias(value)
        except ValueError as exc:
            raise ValueError(f"tipo Arrow '{value}' inválido") from exc
        return value


class SilverToGoldSpec(_Spec):
    columns: list[GoldColumn] = Field(alias="schema", min_length=1)


class RedshiftSpec(_Spec):
    database: str
    schema_name: str = Field(alias="schema")
    table: str
    load_mode: Literal["replace_partition"]

    @property
    def qualified_table(self) -> str:
        return f"{self.schema_name}.{self.table}"


class Contract(_Spec):
    """Contrato de um pipeline (`dags/xp/contracts/<pipeline>.yaml`)."""

    pipeline: str
    description: str | None = None
    schedule: str | None = None
    owner: str = "dw"
    retries: int = 0
    tags: list[str] = []
    s3: S3Spec
    bronze_to_silver: BronzeToSilverSpec
    silver_to_gold: SilverToGoldSpec
    redshift: RedshiftSpec

    def layer(self) -> "Layer":
        from include.etl.layer import Layer

        return Layer(self.s3.path, self.s3.file)

    def to_xcom(self) -> dict:
        """Formato serializável em JSON, para `partial(contract=...)`.
        Volta a ser `Contract` com `Contract.model_validate`."""
        return self.model_dump(mode="json", by_alias=True)
