"""
Configuration settings for EmendoAI
Load from environment variables with fallback defaults
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if exists
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# Database settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DEFAULT_DB = os.getenv("POSTGRES_DEFAULT_DB", "postgres")

# LLM settings
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "4096"))

# Safety settings
DELETE_ROWS_THRESHOLD = int(os.getenv("DELETE_ROWS_THRESHOLD", "5"))
MAX_GENERATION_RETRIES = int(os.getenv("MAX_GENERATION_RETRIES", "3"))
MAX_EXECUTION_RETRIES = int(os.getenv("MAX_EXECUTION_RETRIES", "2"))

# Allowed SQL operations (whitelist)
ALLOWED_SQL_KEYWORDS = [
    "SELECT", "INSERT", "UPDATE", "DELETE", 
    "CREATE", "DROP", "ALTER", "TRUNCATE"
]

# Blocked SQL patterns for security
BLOCKED_SQL_PATTERNS = [
    r";\s*--",  # Comment injection
    r"union\s+select",  # UNION injection
    r"drop\s+database",  # Blocked (too dangerous)
    r"xp_",  # MSSQL extended stored procedures
    r"sp_",  # SQL Server stored procedures
    r"exec\s*\(",  # Execution of dynamic SQL
    r"execute\s*\(",  # Execution of dynamic SQL
]
