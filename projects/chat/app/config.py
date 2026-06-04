from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    anthropic_api_key: str
    chat_db_dsn: str = "postgresql://ngods:ngods@postgres:5432/chat"
    cube_api_url: str = "http://cube:4000"
    cube_api_secret: str = "cube"
    dbt_manifest_path: str = "/dbt/gold_vnstock/target/manifest.json"

    llm_router_model: str = "claude-haiku-4-5-20251001"
    llm_generator_model: str = "claude-sonnet-4-6"

    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384

    retrieval_top_k: int = 12
    cube_max_rows: int = 1000

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
