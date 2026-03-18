"""
Connection manager for handling various source system connections.
Manages connection pooling and credential security.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
import json


@dataclass
class ConnectionConfig:
    """Connection configuration data class."""
    source_type: str
    connection_params: Dict[str, Any]
    credentials: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding sensitive credentials)."""
        return {
            'source_type': self.source_type,
            'connection_params': self.connection_params,
            'credentials': {k: '***' for k in self.credentials.keys()}  # Mask credentials
        }


class BaseConnector(ABC):
    """Abstract base class for source connectors."""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._connection = None
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish connection to the source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        pass
    
    @abstractmethod
    def read_data(self, query_or_path: str, options: Optional[Dict] = None) -> Any:
        """Read data from the source."""
        pass


class JDBCConnector(BaseConnector):
    """Connector for JDBC sources (PostgreSQL, MySQL, SQL Server, etc.)."""
    
    def connect(self) -> Any:
        """Create JDBC connection string and return connection parameters."""
        conn = self.config.connection_params
        creds = self.config.credentials
        
        jdbc_url = f"jdbc:{conn.get('driver_type', 'postgresql')}://" \
                   f"{conn['host']}:{conn.get('port', 5432)}/{conn['database']}"
        
        self._connection = {
            'url': jdbc_url,
            'user': creds.get('username'),
            'password': creds.get('password'),
            'driver': conn.get('driver', 'org.postgresql.Driver')
        }
        return self._connection
    
    def disconnect(self) -> None:
        """JDBC connections are managed by Spark, no explicit disconnect needed."""
        self._connection = None
    
    def test_connection(self) -> bool:
        """Test JDBC connection."""
        try:
            # Attempt a simple query
            test_query = "(SELECT 1) AS test"
            spark.read \
                .format("jdbc") \
                .option("url", self._connection['url']) \
                .option("dbtable", test_query) \
                .option("user", self._connection['user']) \
                .option("password", self._connection['password']) \
                .load()
            return True
        except Exception as e:
            print(f"JDBC connection test failed: {e}")
            return False
    
    def read_data(self, query_or_path: str, options: Optional[Dict] = None) -> Any:
        """Read data from JDBC source."""
        reader = spark.read \
            .format("jdbc") \
            .option("url", self._connection['url']) \
            .option("dbtable", query_or_path) \
            .option("user", self._connection['user']) \
            .option("password", self._connection['password'])
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()


class KafkaConnector(BaseConnector):
    """Connector for Kafka streaming sources."""
    
    def connect(self) -> Any:
        """Configure Kafka connection."""
        conn = self.config.connection_params
        creds = self.config.credentials
        
        self._connection = {
            'bootstrap_servers': conn['bootstrap_servers'],
            'security_protocol': conn.get('security_protocol', 'PLAINTEXT'),
            'sasl_mechanism': creds.get('sasl_mechanism', 'PLAIN'),
            'username': creds.get('username'),
            'password': creds.get('password')
        }
        return self._connection
    
    def disconnect(self) -> None:
        """Kafka connections are streaming, managed by Spark."""
        self._connection = None
    
    def test_connection(self) -> bool:
        """Test Kafka connection by listing topics."""
        try:
            from confluent_kafka.admin import AdminClient
            
            conf = {'bootstrap.servers': self._connection['bootstrap_servers']}
            admin_client = AdminClient(conf)
            admin_client.list_topics(timeout=10)
            return True
        except Exception as e:
            print(f"Kafka connection test failed: {e}")
            return False
    
    def read_data(self, query_or_path: str, options: Optional[Dict] = None) -> Any:
        """Read data from Kafka topic."""
        # query_or_path is the topic name
        reader = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self._connection['bootstrap_servers']) \
            .option("subscribe", query_or_path) \
            .option("startingOffsets", options.get('startingOffsets', 'latest'))
        
        # Add security options if configured
        if self._connection['security_protocol'] != 'PLAINTEXT':
            reader = reader \
                .option("kafka.security.protocol", self._connection['security_protocol']) \
                .option("kafka.sasl.mechanism", self._connection['sasl_mechanism']) \
                .option("kafka.sasl.jaas.config", 
                       f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                       f'username="{self._connection['username']}" '
                       f'password="{self._connection['password']}";')
        
        return reader.load()


class CloudStorageConnector(BaseConnector):
    """Connector for cloud storage (Azure Blob, S3, GCS)."""
    
    def connect(self) -> Any:
        """Configure cloud storage connection."""
        conn = self.config.connection_params
        creds = self.config.credentials
        
        self._connection = {
            'storage_type': conn.get('storage_type', 'azure'),
            'storage_account': conn.get('storage_account'),
            'container': conn.get('container'),
            'credentials': creds
        }
        
        # Set up Spark configuration for cloud storage
        if self._connection['storage_type'] == 'azure':
            spark.conf.set(
                f"fs.azure.account.auth.type.{conn['storage_account']}.dfs.core.windows.net",
                "OAuth"
            )
            spark.conf.set(
                f"fs.azure.account.oauth.provider.type.{conn['storage_account']}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.id.{conn['storage_account']}.dfs.core.windows.net",
                creds.get('client_id')
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.secret.{conn['storage_account']}.dfs.core.windows.net",
                creds.get('client_secret')
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.endpoint.{conn['storage_account']}.dfs.core.windows.net",
                f"https://login.microsoftonline.com/{creds.get('tenant_id')}/oauth2/token"
            )
        
        return self._connection
    
    def disconnect(self) -> None:
        """No explicit disconnect needed for cloud storage."""
        self._connection = None
    
    def test_connection(self) -> bool:
        """Test cloud storage connection by listing files."""
        try:
            path = self._get_full_path("")
            spark.read.format("delta").load(path).limit(1)
            return True
        except Exception as e:
            print(f"Cloud storage connection test failed: {e}")
            return False
    
    def read_data(self, query_or_path: str, options: Optional[Dict] = None) -> Any:
        """Read data from cloud storage path."""
        full_path = self._get_full_path(query_or_path)
        
        reader = spark.read
        if options:
            file_format = options.get('format', 'delta')
            reader = reader.format(file_format)
            
            for key, value in options.items():
                if key != 'format':
                    reader = reader.option(key, value)
        else:
            reader = reader.format("delta")
        
        return reader.load(full_path)
    
    def _get_full_path(self, relative_path: str) -> str:
        """Construct full path for cloud storage."""
        if self._connection['storage_type'] == 'azure':
            return f"abfss://{self._connection['container']}@" \
                   f"{self._connection['storage_account']}.dfs.core.windows.net/{relative_path}"
        elif self._connection['storage_type'] == 's3':
            return f"s3a://{self._connection['container']}/{relative_path}"
        elif self._connection['storage_type'] == 'gcs':
            return f"gs://{self._connection['container']}/{relative_path}"
        else:
            raise ValueError(f"Unsupported storage type: {self._connection['storage_type']}")


class APIConnector(BaseConnector):
    """Connector for REST API sources."""
    
    def connect(self) -> Any:
        """Configure API connection."""
        conn = self.config.connection_params
        creds = self.config.credentials
        
        self._connection = {
            'base_url': conn['base_url'],
            'api_version': conn.get('api_version', 'v1'),
            'auth_type': creds.get('auth_type', 'oauth'),
            'credentials': creds
        }
        return self._connection
    
    def disconnect(self) -> None:
        """No persistent connection for REST APIs."""
        self._connection = None
    
    def test_connection(self) -> bool:
        """Test API connection."""
        try:
            import requests
            
            headers = self._get_auth_headers()
            response = requests.get(
                f"{self._connection['base_url']}/services/data/"
                f"{self._connection['api_version']}/",
                headers=headers,
                timeout=30
            )
            return response.status_code == 200
        except Exception as e:
            print(f"API connection test failed: {e}")
            return False
    
    def read_data(self, query_or_path: str, options: Optional[Dict] = None) -> Any:
        """Read data from API endpoint."""
        # This would typically use a specific API client
        # For now, return empty DataFrame
        return spark.createDataFrame([], StructType([]))
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        creds = self._connection['credentials']
        
        if self._connection['auth_type'] == 'oauth':
            return {
                'Authorization': f"Bearer {creds.get('access_token', '')}",
                'Content-Type': 'application/json'
            }
        elif self._connection['auth_type'] == 'basic':
            import base64
            auth_str = base64.b64encode(
                f"{creds.get('username')}:{creds.get('password')}".encode()
            ).decode()
            return {
                'Authorization': f"Basic {auth_str}",
                'Content-Type': 'application/json'
            }
        else:
            return {'Content-Type': 'application/json'}


class ConnectionManager:
    """
    Manages connections to various source systems.
    Provides connection pooling and lifecycle management.
    """
    
    def __init__(self):
        self._connections: Dict[str, BaseConnector] = {}
        self._connection_pool: Dict[str, Any] = {}
    
    def get_connector(self, source_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Get or create a connector for the specified source type.
        
        Args:
            source_type: Type of source (jdbc, kafka, cloud_storage, api)
            config: Connection configuration
            
        Returns:
            Configured connector instance
        """
        conn_config = ConnectionConfig(
            source_type=source_type,
            connection_params=config.get('connection', {}),
            credentials=config.get('credentials', {})
        )
        
        # Create appropriate connector
        if source_type == 'jdbc':
            connector = JDBCConnector(conn_config)
        elif source_type == 'kafka':
            connector = KafkaConnector(conn_config)
        elif source_type == 'cloud_storage':
            connector = CloudStorageConnector(conn_config)
        elif source_type == 'api':
            connector = APIConnector(conn_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Connect and cache
        connector.connect()
        return connector
    
    def test_all_connections(self, config: Dict[str, Any]) -> Dict[str, bool]:
        """
        Test all configured connections.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Dictionary mapping source names to connection test results
        """
        results = {}
        
        for source in config.get('sources', []):
            try:
                connector = self.get_connector(source['type'], source)
                results[source['name']] = connector.test_connection()
            except Exception as e:
                print(f"Connection test failed for {source['name']}: {e}")
                results[source['name']] = False
        
        return results
    
    def close_all(self) -> None:
        """Close all managed connections."""
        for connector in self._connections.values():
            connector.disconnect()
        self._connections.clear()
        self._connection_pool.clear()
