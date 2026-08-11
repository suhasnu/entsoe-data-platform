from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = Field("local", alias="GRIDFLOW_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # No default: a missing key should fail at startup, not mid request.
    entsoe_api_key: str = Field(..., alias="ENTSOE_API_KEY")

    postgres_host: str = "localhost"
    postgres_port: int = 5433  # 5432 is usually taken by a local Postgres install
    postgres_db: str = "gridflow"
    postgres_user: str = "gridflow_app"
    postgres_password: str = "change_me_locally"

    zones: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["DE_LU", "AT", "NL", "FR", "DK_1", "DK_2"]
    )

    @field_validator("zones", mode="before")
    @classmethod
    def split_csv(cls, value: object) -> object:
        # Env vars arrive as strings; without this pydantic tries to parse JSON.
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()