"""
Unit tests for the DLT Ingestion Framework.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.config_loader import ConfigLoader, ConfigManager
from audit.audit_logger import AuditLogger


class TestConfigLoader(unittest.TestCase):
    """Test cases for ConfigLoader."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_config = """
global:
  catalog: "test_catalog"
  schema_prefix: "test_bronze"
  checkpoint_root: "/test/checkpoints"
  audit_schema: "test_audit"

sources:
  - name: "test_jdbc"
    type: "jdbc"
    connection:
      host: "localhost"
      port: 5432
      database: "test_db"
    credentials:
      username: "test_user"
      password: "test_pass"
    tables:
      - name: "test_table"
        source_table: "public.test"
        primary_key: "id"
        load_type: "full"
"""
        self.config_path = "/tmp/test_config.yaml"
        with open(self.config_path, 'w') as f:
            f.write(self.test_config)
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
    
    def test_load_config(self):
        """Test loading configuration from YAML."""
        loader = ConfigLoader(self.config_path)
        config = loader.load()
        
        self.assertEqual(config['global']['catalog'], 'test_catalog')
        self.assertEqual(len(config['sources']), 1)
        self.assertEqual(config['sources'][0]['name'], 'test_jdbc')
    
    def test_get_source(self):
        """Test retrieving a specific source."""
        loader = ConfigLoader(self.config_path)
        loader.load()
        
        source = loader.get_source('test_jdbc')
        self.assertIsNotNone(source)
        self.assertEqual(source['type'], 'jdbc')
        
        # Test non-existent source
        source = loader.get_source('non_existent')
        self.assertIsNone(source)
    
    def test_get_table(self):
        """Test retrieving a specific table."""
        loader = ConfigLoader(self.config_path)
        loader.load()
        
        table = loader.get_table('test_jdbc', 'test_table')
        self.assertIsNotNone(table)
        self.assertEqual(table['name'], 'test_table')
        self.assertEqual(table['primary_key'], 'id')
    
    def test_validate_config(self):
        """Test configuration validation."""
        # Test missing global section
        invalid_config = """
sources: []
"""
        with open(self.config_path, 'w') as f:
            f.write(invalid_config)
        
        loader = ConfigLoader(self.config_path)
        with self.assertRaises(ValueError):
            loader.load()
    
    def test_validate_source(self):
        """Test source validation."""
        invalid_config = """
global:
  catalog: "test"
  schema_prefix: "test"
  checkpoint_root: "/test"
  audit_schema: "audit"

sources:
  - name: "invalid_source"
    type: "unknown_type"
    tables: []
"""
        with open(self.config_path, 'w') as f:
            f.write(invalid_config)
        
        loader = ConfigLoader(self.config_path)
        with self.assertRaises(ValueError):
            loader.load()


class TestAuditLogger(unittest.TestCase):
    """Test cases for AuditLogger."""
    
    @patch('audit.audit_logger.SparkSession')
    def setUp(self, mock_spark_class):
        """Set up test fixtures."""
        self.mock_spark = MagicMock()
        mock_spark_class.getActiveSession.return_value = self.mock_spark
        
        self.audit_logger = AuditLogger(
            catalog='test_catalog',
            schema='test_audit'
        )
    
    def test_initialize_audit_tables(self):
        """Test audit table initialization."""
        # Verify that CREATE TABLE statements were executed
        calls = self.mock_spark.sql.call_args_list
        create_calls = [call for call in calls if 'CREATE TABLE' in str(call)]
        
        # Should create 4 audit tables
        self.assertGreaterEqual(len(create_calls), 4)
    
    def test_log_batch_start(self):
        """Test logging batch start."""
        self.audit_logger.log_batch_start(
            batch_id='test_batch_001',
            source_name='test_source',
            table_name='test_table'
        )
        
        # Verify INSERT was called
        calls = self.mock_spark.sql.call_args_list
        insert_calls = [call for call in calls if 'INSERT INTO' in str(call)]
        self.assertGreater(len(insert_calls), 0)
    
    def test_log_batch_success(self):
        """Test logging batch success."""
        self.audit_logger.log_batch_success(
            batch_id='test_batch_001',
            source_name='test_source',
            table_name='test_table',
            records_read=1000,
            records_written=995
        )
        
        # Verify UPDATE was called
        calls = self.mock_spark.sql.call_args_list
        update_calls = [call for call in calls if 'UPDATE' in str(call)]
        self.assertGreater(len(update_calls), 0)
    
    def test_log_batch_failure(self):
        """Test logging batch failure."""
        self.audit_logger.log_batch_failure(
            batch_id='test_batch_001',
            source_name='test_source',
            table_name='test_table',
            error_message='Connection timeout'
        )
        
        # Verify UPDATE was called with error message
        calls = self.mock_spark.sql.call_args_list
        update_calls = [call for call in calls if 'UPDATE' in str(call)]
        self.assertGreater(len(update_calls), 0)
    
    def test_log_data_quality_check(self):
        """Test logging data quality check."""
        self.audit_logger.log_data_quality_check(
            batch_id='test_batch_001',
            source_name='test_source',
            table_name='test_table',
            rule_name='not_null_check',
            check_type='NOT_NULL',
            records_checked=1000,
            records_passed=995,
            column_name='email',
            severity='error'
        )
        
        # Verify INSERT was called
        calls = self.mock_spark.sql.call_args_list
        insert_calls = [call for call in calls if 'audit_data_quality' in str(call)]
        self.assertGreater(len(insert_calls), 0)


class TestConnectionManager(unittest.TestCase):
    """Test cases for ConnectionManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        from utils.connection_manager import ConnectionManager
        self.manager = ConnectionManager()
    
    def test_get_connector_jdbc(self):
        """Test getting JDBC connector."""
        from utils.connection_manager import JDBCConnector
        
        config = {
            'connection': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'driver_type': 'postgresql'
            },
            'credentials': {
                'username': 'test_user',
                'password': 'test_pass'
            }
        }
        
        connector = self.manager.get_connector('jdbc', config)
        self.assertIsInstance(connector, JDBCConnector)
    
    def test_get_connector_invalid_type(self):
        """Test getting connector with invalid type."""
        config = {'connection': {}, 'credentials': {}}
        
        with self.assertRaises(ValueError):
            self.manager.get_connector('invalid_type', config)


class TestIngestionExecutor(unittest.TestCase):
    """Test cases for IngestionExecutor."""
    
    @patch('pipelines.ingestion_pipeline.ConfigLoader')
    @patch('pipelines.ingestion_pipeline.AuditLogger')
    def setUp(self, mock_audit_logger, mock_config_loader):
        """Set up test fixtures."""
        from pipelines.ingestion_pipeline import IngestionExecutor
        
        self.mock_config = {
            'global': {
                'catalog': 'test_catalog',
                'audit_schema': 'test_audit'
            }
        }
        
        mock_config_loader.return_value.load.return_value = self.mock_config
        
        self.executor = IngestionExecutor()
    
    def test_generate_batch_id(self):
        """Test batch ID generation."""
        batch_id = self.executor._generate_batch_id()
        
        # Should start with 'batch_' and contain timestamp
        self.assertTrue(batch_id.startswith('batch_'))
        self.assertIn('_', batch_id)


if __name__ == '__main__':
    unittest.main()
