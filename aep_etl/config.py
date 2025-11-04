# aep_etl/config.py
"""
Configuration loader.
Reads environment variables from `.env` (via python-dotenv).
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

def _bool(env: str, default: bool = True) -> bool:
    """Helper to convert env vars like 'true', '1' into boolean."""
    v = os.getenv(env, str(default)).lower()
    return v in {"1", "true", "yes", "y"}

@dataclass(frozen=True)
class Settings:
    # MongoDB connection
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.getenv("MONGO_DB", "aep")
    mongo_collection: str = os.getenv("MONGO_COLLECTION", "energy_metrics")

    # Portal base URL
    base_url: str = os.getenv("AEP_BASE_URL", "https://africa-energy-portal.org")

    # Playwright settings
    headless: bool = _bool("HEADLESS", True)

    # Scraping parameters
    throttle_ms: int = int(os.getenv("THROTTLE_MS", "300"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))

# Singleton settings instance
SETTINGS = Settings()
