import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    from include.etl.layer import Layer

logger = logging.getLogger(__name__)


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
    """Na bronze/silver:
    `{path}/year=AAAA/[cnpj=<cnpj>/][<partição>/]{file}_AAAAMMDD.<ext>`.
    `cnpj`: uma pasta por CNPJ. `partitioned`: subpastas de partição."""

    path: str
    file: str
    cnpj: bool = False
    partitioned: bool = False


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


class _VersionedSpec(_Spec):
    versions: list = Field(min_length=1)

    def version_for(self, file_name: str):
        """Versão com o maior `from` que seja <= data do arquivo."""
        dt = file_date(file_name)
        eligible = [version for version in self.versions if version.valid_from <= dt]
        if not eligible:
            raise ValueError(
                f"Nenhuma versão do contrato vale para {file_name} ({dt}); "
                f"versões a partir de: {sorted(v.valid_from for v in self.versions)}."
            )
        return max(eligible, key=lambda version: version.valid_from)


class BronzeToSilverSpec(_VersionedSpec):
    versions: list[ContractVersion] = Field(min_length=1)


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


FILE_DATE_SOURCES = ("file_date", "dt_ref")


class Condition(_Spec):
    """Linhas em que `column` (coluna da gold) é igual a `equals`."""

    column: str
    equals: str


class EnrichmentColumn(_Spec):
    """Coluna da gold que não vem da silver.

    - `from: file_date` (ou `dt_ref`, a mesma data): data de referência do
      arquivo, como data ou, com `format`, como inteiro (ex.: `%Y%m%d` →
      20260115).
    - `from: <coluna>`: cópia de uma coluna da gold (pode ser a própria
      coluna, para transformá-la). Opções, aplicadas nesta ordem:
      - `map`: troca valores (ex.: `{F: PF}`); os demais ficam como estão;
      - `upper`: converte para maiúsculas;
      - `negative_when`: o valor fica negativo nas linhas da condição (ex.:
        débito com valor positivo vira negativo)."""

    name: str
    source: str = Field(alias="from")
    format: str | None = None
    mapping: dict[str, str] = Field(default={}, alias="map")
    upper: bool = False
    negative_when: Condition | None = None
    description: str | None = None

    @property
    def from_file_date(self) -> bool:
        return self.source in FILE_DATE_SOURCES

    @model_validator(mode="after")
    def _options_match_source(self) -> "EnrichmentColumn":
        if self.from_file_date and (self.negative_when or self.mapping or self.upper):
            raise ValueError(
                "`map`, `upper` e `negative_when` só valem com `from: <coluna>`"
            )
        if not self.from_file_date and self.format:
            raise ValueError("`format` só vale com `from: file_date` ou `dt_ref`")
        return self

    def value(self, dt_ref: date) -> date | int:
        if self.format:
            return int(dt_ref.strftime(self.format))
        return dt_ref


class SilverToGoldVersion(_Spec):
    valid_from: date = Field(alias="from")
    enrichment: list[EnrichmentColumn] = []
    columns: list[GoldColumn] = Field(alias="schema", min_length=1)

    @model_validator(mode="after")
    def _enrichment_in_schema(self) -> "SilverToGoldVersion":
        names = {column.name for column in self.columns}
        missing = [
            column.name for column in self.enrichment if column.name not in names
        ]
        if missing:
            raise ValueError(f"colunas de `enrichment` fora do `schema`: {missing}")

        # Coluna derivada lê colunas que vêm da silver (ou ela mesma), não
        # outras derivadas.
        enriched = {column.name for column in self.enrichment}
        for column in self.enrichment:
            if column.from_file_date:
                continue
            used = [column.source]
            if column.negative_when:
                used.append(column.negative_when.column)
            invalid = [
                name
                for name in used
                if name not in names or (name in enriched and name != column.name)
            ]
            if invalid:
                raise ValueError(
                    f"`{column.name}` usa colunas que não vêm da silver: {invalid}"
                )
        return self


class SilverToGoldSpec(_VersionedSpec):
    versions: list[SilverToGoldVersion] = Field(min_length=1)


class RedshiftSpec(_Spec):
    database: str
    schema_name: str = Field(alias="schema")
    table: str
    load_mode: Literal["replace_partition"]
    partition_key: Literal["dt_ref"] = "dt_ref"

    @property
    def qualified_table(self) -> str:
        return f"{self.schema_name}.{self.table}"


class VolumeCheck(_Spec):
    """Linhas do arquivo <= média das últimas `last` dt_ref carregadas
    x `max_ratio`."""

    check: Literal["volume"]
    max_ratio: float = 1.5
    last: int = 4


class HistoryRangeCheck(_Spec):
    """`metric` (expressão SQL agregada, ex.: `SUM(vlr_net_m)`) dentro de
    fechamento do mês anterior x (1 ± maior variação mensal dos últimos
    `months` meses x `multiplier`)."""

    check: Literal["history_range"]
    name: str
    metric: str
    multiplier: float = 2
    months: int = 12


QualityCheck = Annotated[VolumeCheck | HistoryRangeCheck, Field(discriminator="check")]


class SlaSpec(_Spec):
    """Prazo em `days` (dias corridos) ou `hours`. Em
    `alerts.redshift_delay.sla` é o prazo de entrega ao consumidor: o arquivo
    tem que estar no Redshift até esse prazo depois de chegar na bronze."""

    days: int | None = Field(default=None, gt=0)
    hours: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _one_unit(self) -> "SlaSpec":
        if (self.days is None) == (self.hours is None):
            raise ValueError("informe `days` ou `hours` (só um dos dois)")
        return self

    @property
    def deadline(self) -> timedelta:
        return timedelta(days=self.days or 0, hours=self.hours or 0)

    def __str__(self) -> str:
        if self.days:
            return f"{self.days} dia(s)"
        return f"{self.hours} hora(s)"


class SourceSpec(_Spec):
    """Quem gera o dado na origem. Documentação: o pipeline não lê."""

    description: str
    disponibilization_sla: SlaSpec | None = None
    owner: str
    contact: str


class ExtractionSpec(_Spec):
    """Como o arquivo chega na bronze. Documentação: o pipeline não lê.
    `reprocessing_window_days`: quantos dias para trás o SmartCheck aceita
    dt_ref (a origem pode alterar dados já publicados)."""

    description: str
    capture_method: Literal["manual", "api"]
    capture_sla: SlaSpec | None = None
    contact: str
    reprocessing_window_days: int | None = Field(default=None, gt=0)


class Recipients(_Spec):
    to: list[str] = []
    cc: list[str] = []


class S3DelayAlert(Recipients):
    """Arquivo novo não chega na bronze há mais de
    `max_days_without_update` dias."""

    max_days_without_update: int | None = Field(default=None, gt=0)


class RedshiftDelayAlert(Recipients):
    """Arquivo chegou na bronze e não foi carregado no Redshift dentro do
    `sla`."""

    sla: SlaSpec | None = None


class AlertsSpec(_Spec):
    s3_delay: S3DelayAlert | None = None
    redshift_delay: RedshiftDelayAlert | None = None
    job_failure: Recipients | None = None


class Contract(_Spec):
    """Contrato de um pipeline (`dags/xp/contracts/<pipeline>.yaml`)."""

    pipeline: str
    display_name: str
    business_justification: str
    description: str | None = None
    schedule: str | None = None
    sensor_interval: int | None = Field(default=None, gt=0)
    owner: str = "dw"
    retries: int = 0
    tags: list[str] = []
    source: SourceSpec | None = None
    extraction: ExtractionSpec | None = None
    s3: S3Spec
    redshift: RedshiftSpec
    bronze_to_silver: BronzeToSilverSpec
    silver_to_gold: SilverToGoldSpec
    quality: list[QualityCheck] = []
    alerts: AlertsSpec = AlertsSpec()

    @model_validator(mode="after")
    def _one_trigger(self) -> "Contract":
        if self.schedule and self.sensor_interval:
            raise ValueError("use `schedule` ou `sensor_interval`, não os dois")
        return self

    @property
    def sla(self) -> SlaSpec | None:
        """Prazo de entrega ao consumidor (`alerts.redshift_delay.sla`)."""
        delay = self.alerts.redshift_delay
        return delay.sla if delay else None

    def layer(self) -> "Layer":
        from include.etl.layer import Layer

        return Layer(
            self.s3.path,
            self.s3.file,
            cnpj=self.s3.cnpj,
            partitioned=self.s3.partitioned,
        )

    def to_xcom(self) -> dict:
        """Formato serializável em JSON, para `partial(contract=...)`.
        Volta a ser `Contract` com `Contract.model_validate`."""
        return self.model_dump(mode="json", by_alias=True)


def load_contracts(directory: Path) -> list[Contract]:
    """Contratos válidos de `directory/*.yaml`; os inválidos são logados e
    ignorados."""
    contracts = []
    for path in sorted(directory.glob("*.yaml")):
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            contracts.append(Contract.model_validate(raw))
        except (yaml.YAMLError, ValidationError) as exc:
            logger.error(f"Contrato inválido, ignorado: {path.name}: {exc}")
    return contracts
