import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AzureConfig:
    """Azure Blob Storage configuration."""
    connection_string: str
    container: str
    prefix: Optional[str] = None

    @classmethod
    def from_env(cls) -> Optional['AzureConfig']:
        """Load config from environment variables. Returns None if not configured."""
        conn_str = os.getenv('AZURE_CONNECTION_STRING')
        container = os.getenv('AZURE_CONTAINER')

        if not conn_str or not container:
            return None

        return cls(
            connection_string=conn_str,
            container=container,
            prefix=os.getenv('AZURE_PREFIX'),
        )
