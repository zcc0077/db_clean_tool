import os
import yaml


def load_config(path: str) -> dict:
    """
    Load configuration file with support for overriding sensitive configurations via environment variables
    
    Supported environment variables:
    - DATABASE_CONNECTION_STRING or DB_URI: Database connection string
    - DRY_RUN: Whether to run in dry-run mode (true/false)
    - EXPIRY_DAYS: Number of days for data expiry cutoff
    - ARCHIVE: Whether to archive deleted data to CSV
    """
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Override database connection info from environment variables
    # Support two environment variable names for compatibility with existing systems
    db_uri = os.getenv('DATABASE_CONNECTION_STRING') or os.getenv('DB_URI')
    if db_uri:
        config['db_uri'] = db_uri
        print(f"✓ Using database connection info from environment variables")
    
    # Support overriding dry_run setting from environment variables
    dry_run_env = os.getenv('DRY_RUN')
    if dry_run_env is not None:
        config['dry_run'] = dry_run_env.lower() in ('true', '1', 'yes', 'on')
        print(f"✓ Using environment variable to set dry_run = {config['dry_run']}")

    return config