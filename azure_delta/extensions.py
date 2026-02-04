import logging
import duckdb
from typing import Optional
from .config import AzureConfig

logger = logging.getLogger(__name__)


def setup_azure_extensions(conn: duckdb.DuckDBPyConnection, config: Optional[AzureConfig] = None) -> bool:
    """
    Install and configure Azure/Delta extensions on a DuckDB connection.

    If config is None, extensions are installed but no Azure secret is created.
    This allows the server to run without Azure connectivity.
    """
    try:
        conn.execute("INSTALL azure;")
        conn.execute("INSTALL delta;")
        conn.execute("LOAD azure;")
        conn.execute("LOAD delta;")
        logger.info("Azure and Delta extensions loaded successfully")

        if config:
            conn_str_escaped = config.connection_string.replace("'", "''")
            conn.execute(f"""
                CREATE SECRET IF NOT EXISTS azure_secret (
                    TYPE AZURE,
                    CONNECTION_STRING '{conn_str_escaped}'
                );
            """)
            logger.info("Azure secret configured successfully")
        else:
            logger.info("No Azure config provided - running without Azure connectivity")

        return True
    except Exception as e:
        logger.warning(f"Failed to initialize Azure/Delta extensions: {e}")
        logger.warning("Server will continue without Azure/Delta support")
        return False
