import socket
from loguru import logger
from datetime import datetime
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Dict

from src.utility.SdUtility import getConfigIni
from src.model import (
    ElasticConfig,
    EngineConfig,
    QueryConfig,
    DateFormats,
    SortOrder,
    IsoFormat
)


class ConfigValidator:
    """Base configuration validator with common validation methods."""
    @staticmethod
    def validate_url(url: str) -> List[str]:
        """Validate URL format and hostname resolution."""
        errors = []
        try:
            parsed_url = urlparse(url)
            if parsed_url.scheme not in ["http", "https"]:
                errors.append("URL must start with http:// or https://")
            
            try:
                socket.gethostbyname(parsed_url.hostname)
            except socket.gaierror:
                errors.append(f"Cannot resolve hostname: {parsed_url.hostname}")
        except Exception as e:
            errors.append(f"Invalid URL format: {str(e)}")
        return errors

class SdValidator:
    """
    Configuration validator for SD application.
    
    Validates and manages configuration for Elasticsearch, Engine, and Query components.
    """
    
    def __init__(self):
        """
        Initialize the validator with configuration dictionary.
        
        Args:
            config: Configuration dictionary from ini file
        """
        self.logger = logger
        self.config = getConfigIni()
        self._elastic_config: Optional[ElasticConfig] = None
        self._engine_config: Optional[EngineConfig] = None
        self._query_config: Optional[QueryConfig] = None

    def validate_elastic_config(self) -> Tuple[bool, List[str]]:
        """
        Validate Elasticsearch configuration section.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            if 'elastic' not in self.config:
                return False, ["Missing [elastic] section in config file"]
            
            elastic_config = self.config['elastic']
            
            # Validate required fields
            required_fields = ElasticConfig.__annotations__.keys()
            missing_fields = [field for field in required_fields if field not in elastic_config]
            if missing_fields:
                return False, [f"Missing required field: {field}" for field in missing_fields]
            
            # Validate URL
            url_errors = ConfigValidator.validate_url(elastic_config['es_url'])
            errors.extend(url_errors)
            
            # Validate timeout
            try:
                timeout = int(elastic_config['es_timeout'])
                if timeout <= 0:
                    errors.append("es_timeout must be a positive integer")
                elif timeout > 86400:
                    errors.append("es_timeout must be less than equal to 86400 seconds")
            except ValueError:
                errors.append("es_timeout must be an integer")
            
            if not errors:
                self._elastic_config = ElasticConfig(**elastic_config)
            
            return len(errors) == 0, errors
            
        except Exception as e:
            return False, [f"Error validating elastic config: {str(e)}"]

    def validate_engine_config(self) -> Tuple[bool, List[str]]:
        """
        Validate Engine configuration section.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            if 'engine' not in self.config:
                return False, ["Missing [engine] section in config file"]
            
            engine_config = self.config['engine']
            
            # Validate batch size
            try:
                batch_size = int(engine_config['batch_size'])
                if batch_size < 1:
                    errors.append("batch_size must be greater than equal to 1")
                elif batch_size > 1000:
                    errors.append("batch_size must be less than equal to 1000")
            except ValueError:
                errors.append("batch_size must be an integer")
            
            # Validate max retry connection
            try:
                max_retry_connection = int(engine_config['max_retry_connection'])
                if max_retry_connection < 1:
                    errors.append("max_retry_connection must be greater than to 1")
                elif max_retry_connection > 10:
                    errors.append("max_retry_connection must be less than equal to 10")
            except ValueError:
                errors.append("max_retry_connection must be an integer")

            # Validate date format
            try:
                format_date = engine_config['format_date']
                DateFormats(format_date)
            except ValueError:
                errors.append(f"Invalid format_date. Must be one of: {', '.join(f.value for f in DateFormats)}")
            
            if not errors:
                self._engine_config = EngineConfig(
                    batch_size=batch_size,
                    max_retry_connection=max_retry_connection,
                    format_date=format_date
                )
            
            return len(errors) == 0, errors
            
        except Exception as e:
            return False, [f"Error validating engine config: {str(e)}"]

    def validate_query_config(self) -> Tuple[bool, List[str]]:
        """
        Validate Query configuration section.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            if 'query' not in self.config:
                return False, ["Missing [query] section in config file"]
            
            query_config = self.config['query']
            
            # Validate used_query
            used_query = query_config.get('used_query', 'no').lower()
            if used_query not in ['yes', 'no']:
                errors.append("used_query must be 'yes' or 'no'")
            
            # Validate dates if present
            for date_field in ['gte', 'lte']:
                if date_field in query_config and query_config[date_field]:
                    try:
                        datetime.strptime(query_config[date_field], '%Y-%m-%d')
                    except ValueError:
                        errors.append(f"{date_field} must be in YYYY-MM-DD format")

            # Validate iso format
            if 'iso_format' in query_config:
                try:
                    IsoFormat(query_config["iso_format"])
                except ValueError:
                    errors.append(f"Invalid iso_format. Must be one of: {', '.join(f.value for f in IsoFormat)}")

            # Validate sort order
            if 'sort_order' in query_config:
                try:
                    SortOrder(query_config['sort_order'].lower())
                except ValueError:
                    errors.append(f"Invalid sort_order. Must be one of: {', '.join(f.value for f in SortOrder)}")
            
            if not errors:
                self._query_config = QueryConfig(
                    used_query=used_query,
                    gte=query_config.get('gte'),
                    lte=query_config.get('lte'),
                    iso_format=query_config.get('iso_format'),
                    sort_order=query_config.get('sort_order', 'asc').lower()
                )
            
            return len(errors) == 0, errors
            
        except Exception as e:
            return False, [f"Error validating query config: {str(e)}"]

    def get_elastic_config(self) -> Optional[ElasticConfig]:
        """Get validated Elasticsearch configuration."""
        if self._elastic_config is None:
            is_valid, errors = self.validate_elastic_config()
            if not is_valid:
                self.logger.error("Invalid Elasticsearch configuration: %s", errors)
                return None
        return self._elastic_config

    def get_engine_config(self) -> Optional[EngineConfig]:
        """Get validated Engine configuration."""
        if self._engine_config is None:
            is_valid, errors = self.validate_engine_config()
            if not is_valid:
                self.logger.error("Invalid Engine configuration: %s", errors)
                return None
        return self._engine_config

    def get_query_config(self) -> Optional[QueryConfig]:
        """Get validated Query configuration."""
        if self._query_config is None:
            is_valid, errors = self.validate_query_config()
            if not is_valid:
                self.logger.error("Invalid Query configuration: %s", errors)
                return None
        return self._query_config
