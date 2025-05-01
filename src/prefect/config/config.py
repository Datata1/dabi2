from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib.parse import quote_plus # Sicherstellen, dass Passwörter URL-safe sind

class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: str = "5432" 
    DB_NAME: str

    DATABASE_URL: str | None = None
    MAINTENANCE_DATABASE_URL: str | None = None


    def __init__(self, **values):
        super().__init__(**values)
        safe_password = quote_plus(self.DB_PASSWORD)
        if not self.DATABASE_URL:
            self.DATABASE_URL = f"postgresql+psycopg2://{self.DB_USER}:{safe_password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        if not self.MAINTENANCE_DATABASE_URL:
            self.MAINTENANCE_DATABASE_URL = f"postgresql+psycopg2://{self.DB_USER}:{safe_password}@{self.DB_HOST}:{self.DB_PORT}/postgres"


    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

settings = Settings() 