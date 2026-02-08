from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MinIOSettings(BaseSettings):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False

    model_config = SettingsConfigDict(
        env_prefix="MINIO_", env_file=".env", extra="ignore"
    )


class SnowflakeSettings(BaseSettings):
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema_: str = Field(
        validation_alias=AliasChoices(
            "SCHEMA",
            "SCHEMA_",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_SCHEMA_",
        )
    )
    role: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="SNOWFLAKE_",
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )



class PrefectSettings(BaseSettings):
    api_url: str

    model_config = SettingsConfigDict(
        env_prefix="PREFECT_", env_file=".env", extra="ignore"
    )
