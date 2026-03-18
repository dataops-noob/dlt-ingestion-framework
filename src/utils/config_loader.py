"""
Configuration loader for the DLT Ingestion Framework.
Handles YAML configuration parsing and environment variable substitution.
"""

import yaml
import os
import re
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigLoader:
    """
    Loads and validates pipeline configuration from YAML files.
    Supports environment variable substitution and configuration validation.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the config loader.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        
    def load(self) -> Dict[str, Any]:
        """
        Load and parse the configuration file.
        
        Returns:
            Dictionary containing the parsed configuration
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            content = f.read()
        
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        # Parse YAML
        self.config = yaml.safe_load(content)
        
        # Validate configuration
        self._validate_config()
        
        return self.config
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        Substitute environment variables in the configuration content.
        Supports ${VAR_NAME} syntax.
        
        Args:
            content: Raw configuration content
            
        Returns:
            Content with environment variables substituted
        """
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_name = match.group(1)
            value = os.getenv(var_name, '')
            if not value:
                print(f"Warning: Environment variable '{var_name}' not set")
            return value
        
        return re.sub(pattern, replace_var, content)
    
    def _validate_config(self) -> None:
        """
        Validate the loaded configuration.
        Raises ValueError if configuration is invalid.
        """
        required_global = ['catalog', 'schema_prefix', 'checkpoint_root']
        
        if 'global' not in self.config:
            raise ValueError("Configuration missing 'global' section")
        
        global_config = self.config['global']
        for key in required_global:
            if key not in global_config:
                raise ValueError(f"Missing required global config: {key}")
        
        if 'sources' not in self.config:
            raise ValueError("Configuration missing 'sources' section")
        
        if not self.config['sources']:
            raise ValueError("At least one source must be configured")
        
        # Validate each source
        for source in self.config['sources']:
            self._validate_source(source)
    
    def _validate_source(self, source: Dict[str, Any]) -> None:
        """
        Validate a source configuration.
        
        Args:
            source: Source configuration dictionary
        """
        required_fields = ['name', 'type', 'tables']
        
        for field in required_fields:
            if field not in source:
                raise ValueError(f"Source missing required field: {field}")
        
        valid_types = ['jdbc', 'api', 'kafka', 'cloud_storage', 'file']
        if source['type'] not in valid_types:
            raise ValueError(f"Invalid source type '{source['type']}'. Must be one of: {valid_types}")
        
        if not source['tables']:
            raise ValueError(f"Source '{source['name']}' must have at least one table")
        
        # Validate each table
        for table in source['tables']:
            self._validate_table(source['name'], table)
    
    def _validate_table(self, source_name: str, table: Dict[str, Any]) -> None:
        """
        Validate a table configuration.
        
        Args:
            source_name: Name of the parent source
            table: Table configuration dictionary
        """
        if 'name' not in table:
            raise ValueError(f"Table in source '{source_name}' missing required field: name")
        
        valid_load_types = ['full', 'incremental', 'streaming']
        load_type = table.get('load_type', 'full')
        
        if load_type not in valid_load_types:
            raise ValueError(f"Invalid load_type '{load_type}' for table '{table['name']}'. "
                           f"Must be one of: {valid_load_types}")
        
        # Incremental loads require incremental_column
        if load_type == 'incremental' and 'incremental_column' not in table:
            raise ValueError(f"Table '{table['name']}' with load_type='incremental' "
                           f"must specify 'incremental_column'")
    
    def get_source(self, source_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific source.
        
        Args:
            source_name: Name of the source
            
        Returns:
            Source configuration dictionary or None if not found
        """
        for source in self.config.get('sources', []):
            if source['name'] == source_name:
                return source
        return None
    
    def get_table(self, source_name: str, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific table.
        
        Args:
            source_name: Name of the source
            table_name: Name of the table
            
        Returns:
            Table configuration dictionary or None if not found
        """
        source = self.get_source(source_name)
        if not source:
            return None
        
        for table in source.get('tables', []):
            if table['name'] == table_name:
                # Merge source config with table config
                merged = {**source, **table}
                merged['source_name'] = source_name
                return merged
        
        return None
    
    def get_all_tables(self) -> list:
        """
        Get all table configurations across all sources.
        
        Returns:
            List of table configuration dictionaries
        """
        tables = []
        for source in self.config.get('sources', []):
            for table in source.get('tables', []):
                merged = {**source, **table}
                merged['source_name'] = source['name']
                tables.append(merged)
        return tables
    
    def get_global_config(self) -> Dict[str, Any]:
        """
        Get global configuration settings.
        
        Returns:
            Global configuration dictionary
        """
        return self.config.get('global', {})
    
    def get_pipeline_settings(self) -> Dict[str, Any]:
        """
        Get DLT pipeline settings.
        
        Returns:
            Pipeline settings dictionary
        """
        return self.config.get('pipeline_settings', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """
        Get monitoring and alerting configuration.
        
        Returns:
            Monitoring configuration dictionary
        """
        return self.config.get('monitoring', {})
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """
        Get data quality configuration.
        
        Returns:
            Data quality configuration dictionary
        """
        return self.config.get('data_quality', {})


class ConfigManager:
    """
    Manages configuration updates and versioning.
    """
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.loader = ConfigLoader(config_path)
    
    def update_source(self, source_name: str, updates: Dict[str, Any]) -> None:
        """
        Update a source configuration.
        
        Args:
            source_name: Name of the source to update
            updates: Dictionary of updates to apply
        """
        config = self.loader.load()
        
        for i, source in enumerate(config['sources']):
            if source['name'] == source_name:
                config['sources'][i].update(updates)
                break
        else:
            raise ValueError(f"Source '{source_name}' not found")
        
        self._save_config(config)
    
    def add_source(self, source_config: Dict[str, Any]) -> None:
        """
        Add a new source to the configuration.
        
        Args:
            source_config: New source configuration
        """
        config = self.loader.load()
        
        # Check for duplicate names
        for source in config['sources']:
            if source['name'] == source_config['name']:
                raise ValueError(f"Source '{source_config['name']}' already exists")
        
        config['sources'].append(source_config)
        self._save_config(config)
    
    def remove_source(self, source_name: str) -> None:
        """
        Remove a source from the configuration.
        
        Args:
            source_name: Name of the source to remove
        """
        config = self.loader.load()
        config['sources'] = [s for s in config['sources'] if s['name'] != source_name]
        self._save_config(config)
    
    def _save_config(self, config: Dict[str, Any]) -> None:
        """
        Save configuration back to file.
        
        Args:
            config: Configuration dictionary to save
        """
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
