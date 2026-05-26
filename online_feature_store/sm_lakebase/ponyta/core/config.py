from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    AWS_DEFAULT_REGION: str
    AWS_DEFAULT_REGION = "us-east-1"
    DATABRICKS_CLUSTER_HOST: Optional[str] = Field(
        default=None,
        env="DATABRICKS_HOST",
    )
    DATABRICKS_TOKEN: Optional[str] = Field(
        default=None,
        env="DATABRICKS_TOKEN",
    )
    DATABRICKS_SQL_CLUSTER_PATH: Optional[str] = Field(
        default=None,
        env="DATABRICKS_SQL_PATH",
    )
    UNITY_CATALOG: Optional[str] = Field(
        default=None,
        env="UNITY_CATALOG",
    )
    DATABRICKS_CLIENT_ID: Optional[str] = Field(
        default=None,
        env="DATABRICKS_CLIENT_ID",
    )
    DATABRICKS_CLIENT_SECRET: Optional[str] = Field(
        default=None,
        env="DATABRICKS_CLIENT_SECRET",
    )
    LAKSBASE_PREFIX: str
    LAKSBASE_PREFIX = "pidgey"


load_dotenv()
settings = Settings()
