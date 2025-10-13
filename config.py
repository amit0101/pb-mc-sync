"""
Configuration management for Pabau-Mailchimp sync
"""
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Pabau Configuration
    pabau_api_key: str = Field(..., env="PABAU_API_KEY")
    pabau_api_url: str = Field(default="https://api.oauth.pabau.com", env="PABAU_API_URL")
    pabau_company_id: str = Field(default="", env="PABAU_COMPANY_ID")  # Not used with oauth URL
    
    # Mailchimp Configuration
    mailchimp_api_key: str = Field(..., env="MAILCHIMP_API_KEY")
    mailchimp_server_prefix: str = Field(default="us1", env="MAILCHIMP_SERVER_PREFIX")
    mailchimp_list_id: str = Field(..., env="MAILCHIMP_LIST_ID")
    
    # Database Configuration
    database_url: str = Field(..., env="DATABASE_URL")
    
    # Application Settings
    app_env: str = Field(default="production", env="APP_ENV")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Sync Settings
    sync_batch_size: int = Field(default=50, env="SYNC_BATCH_SIZE")  # Pabau API limit is 50 per page
    
    @property
    def mailchimp_api_url(self) -> str:
        """Construct Mailchimp API URL"""
        return f"https://{self.mailchimp_server_prefix}.api.mailchimp.com/3.0"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()

