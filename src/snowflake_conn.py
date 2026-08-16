import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

load_dotenv()


def get_snowflake_connection():
    """
    Return a Snowflake connection using private-key authentication.

    Environment variables used:
      - SNOWFLAKE_USER
      - SNOWFLAKE_ACCOUNT
      - SNOWFLAKE_WAREHOUSE
      - SNOWFLAKE_DATABASE
      - SNOWFLAKE_SCHEMA
      - SNOWFLAKE_PRIVATE_KEY_PATH
      - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional)
    """
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not key_path:
        raise RuntimeError("SNOWFLAKE_PRIVATE_KEY_PATH is not set")

    resolved_path = Path(key_path).expanduser()
    if not resolved_path.exists():
        raise RuntimeError(f"Private key file not found: {resolved_path}")

    with resolved_path.open("rb") as key_file:
        key_data = key_file.read()

    """passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    password = passphrase.encode() if passphrase else None"""

    p_key = serialization.load_pem_private_key(
        key_data,
        password=None,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        private_key=pkb,
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        #database=os.getenv('SNOWFLAKE_DATABASE'),
        #schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    return conn
