import logging
import duckdb
from typing import List, Optional
from .config import AzureConfig

logger = logging.getLogger(__name__)


def discover_delta_tables(conn: duckdb.DuckDBPyConnection, config: Optional[AzureConfig] = None) -> List[str]:
    """
    Discover and register Delta tables from Azure as views.

    If config is None, returns empty list (no-op).
    """
    if not config:
        logger.debug("No Azure config - skipping Delta table discovery")
        return []

    base_path = f"az://{config.container}"
    if config.prefix:
        base_path = f"{base_path}/{config.prefix.strip('/')}"

    logger.info(f"Discovering Delta tables from: {base_path}")
    tables = []
    try:
        result = conn.execute(f"""
            SELECT DISTINCT regexp_extract(filename, '([^/]+)/_delta_log/', 1) as table_name
            FROM glob('{base_path}/*/_delta_log/*')
            WHERE table_name IS NOT NULL
        """).fetchall()

        for (table_name,) in result:
            if table_name:
                table_path = f"{base_path}/{table_name}"
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM delta_scan('{table_path}')
                """)
                tables.append(table_name)
                logger.info(f"Registered Delta table: {table_name}")

        if tables:
            logger.info(f"Discovered {len(tables)} Delta table(s)")
        else:
            logger.info("No Delta tables found")
    except Exception as e:
        logger.warning(f"Delta table discovery failed: {e}")

    return tables
