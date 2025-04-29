import logging
from typing import Optional

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # Supabase Configuration
    supabase_url: HttpUrl = Field(..., description="URL for the Supabase project.")
    supabase_key: str = Field(..., description="Anon key for the Supabase project.")
    supabase_service_key: Optional[str] = Field(
        None, description="Service role key for Supabase (use with caution!)."
    )

    # Sportsbook Credentials
    crabsports_cookie: str = Field(..., description="Cookie string for Crab Sports.")
    pinnacle_api_key: str = Field(..., description="API key for Pinnacle.")
    # tbd_book_api_key: Optional[str] = Field(None, description="API key for the TBD book.") # Uncomment when ready

    # Calculation Settings
    kelly_fraction_cap: float = Field(
        0.01,  # Default value if not in .env
        ge=0,
        le=1,
        description="Maximum Kelly fraction to recommend (e.g., 0.01 for 1%).",
    )
    min_ev_threshold: float = Field(
        0.01,  # Default value if not in .env
        ge=0,
        description="Minimum positive EV threshold to consider (e.g., 0.01 for 1%).",
    )

    # Logging Configuration
    log_level: str = Field(
        "INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)."
    )

    # Pydantic Settings Configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Betting Strategy Configuration
    KELLY_FRACTION_CAP: float = 0.01
    MIN_EV_THRESHOLD: float = 0.01

    # Supabase Configuration
    SUPABASE_URL: Optional[str] = None
    SUPABASE_KEY: Optional[str] = None

    # Logging Configuration
    LOG_LEVEL: str = "INFO"


def load_settings() -> AppSettings:
    """Loads and validates application settings."""
    try:
        settings = AppSettings()
        log_level_upper = settings.log_level.upper()
        # Validate log_level even if loaded from .env
        if log_level_upper not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            logging.warning(
                f"Invalid LOG_LEVEL '{settings.log_level}' found in .env or default. Using INFO."
            )
            settings.log_level = "INFO"
        else:
            settings.log_level = log_level_upper
        return settings
    except Exception as e:
        logging.exception(f"Error loading application settings: {e}")
        raise SystemExit("Failed to load application settings. Exiting.")


settings: AppSettings = load_settings()  # <-- Uncommented
