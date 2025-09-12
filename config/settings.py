"""Configuration settings for the Lead Validation Reporting project."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# DuckDB Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(DATA_DIR / "leads.duckdb"))
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "2GB")

# Salesforce Configuration
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_TOKEN_URL = os.getenv("SF_TOKEN_URL")

# Dashboard Configuration
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "localhost")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8501"))
DASHBOARD_TITLE = os.getenv("DASHBOARD_TITLE", "Lead Validation Dashboard")

# Validation Configuration
VALIDATION_RULES_STRICT = os.getenv("VALIDATION_RULES_STRICT", "true").lower() == "true"
COMPLIANCE_CHECKS_ENABLED = os.getenv("COMPLIANCE_CHECKS_ENABLED", "true").lower() == "true"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))
DUPLICATE_THRESHOLD = float(os.getenv("DUPLICATE_THRESHOLD", "0.85"))
MIN_COMPLETENESS_SCORE = float(os.getenv("MIN_COMPLETENESS_SCORE", "0.7"))

# External Services
CLEARBIT_API_KEY = os.getenv("CLEARBIT_API_KEY")
ZEROBOUNCE_API_KEY = os.getenv("ZEROBOUNCE_API_KEY")

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", str(LOGS_DIR / "validation.log"))

# Required Lead Fields for Validation
REQUIRED_FIELDS = [
    "FirstName", "LastName", "Email", "Phone", "Company", "Status"
]

# Optional but Important Fields
IMPORTANT_FIELDS = [
    "Title", "Industry", "LeadSource", "City", "State", "Country"
]

# Validation Rules Configuration
VALIDATION_RULES = {
    "email": {
        "required": True,
        "format": "email",
        "duplicates_allowed": False
    },
    "phone": {
        "required": True,
        "format": "phone",
        "min_length": 10
    },
    "company": {
        "required": True,
        "min_length": 2,
        "max_length": 255
    },
    "name": {
        "first_name_required": True,
        "last_name_required": True,
        "min_length": 1
    }
}

# Compliance Rules
COMPLIANCE_RULES = {
    "tcpa_consent": True,
    "opt_in_required": True,
    "dnc_check_required": True,
    "data_retention_limit": DATA_RETENTION_DAYS
}
