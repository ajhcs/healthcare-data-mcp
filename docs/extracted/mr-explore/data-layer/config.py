"""
Configuration file system for MR-Explore.

Supports YAML-based configuration with environment variable overrides
and sensible defaults for all application settings.
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any


# Default configuration values
DEFAULT_CONFIG = {
    "database": {
        "default_type": "sqlite",  # sqlite, duckdb, embedded
        "sqlite_path": "data/mr_explore.db",
        "duckdb_pack_path": "data/packs/main",
        "connection_pool_size": 5,
        "connection_timeout": 30,
        "duckdb": {
            "memory_limit": "4GB",
            "threads": 8,
            "enable_object_cache": True,
            "max_memory": "4GB",
            "temp_directory": None,
            "default_order": "ASC",
        },
    },
    "ui": {
        "window_width": 1200,
        "window_height": 800,
        "table_page_size": 100,
        "search_debounce_ms": 300,
        "remember_filters": True,
    },
    "export": {
        "default_format": "csv",  # csv, excel, parquet
        "csv_delimiter": ",",
        "csv_quote_char": '"',
        "batch_size": 10000,
        "max_export_records": 1000000,
    },
    "logging": {
        "enabled": True,
        "level": "INFO",  # DEBUG, INFO, WARNING, ERROR
        "file_path": "logs/mr_explore.log",
        "max_file_size_mb": 10,
        "backup_count": 3,
    },
    "performance": {
        "enable_cache": True,
        "cache_ttl_seconds": 300,
        "max_cache_size_mb": 500,
        "enable_compression": True,
    },
}


@dataclass
class DuckDBConfig:
    """DuckDB-specific configuration settings."""

    memory_limit: str = "4GB"
    threads: int = 8
    enable_object_cache: bool = True
    max_memory: str = "4GB"
    temp_directory: Optional[str] = None
    default_order: str = "ASC"


@dataclass
class DatabaseConfig:
    """Database configuration settings."""

    default_type: str = "sqlite"
    sqlite_path: str = "data/mr_explore.db"
    duckdb_pack_path: str = "data/packs/main"
    connection_pool_size: int = 5
    connection_timeout: int = 30
    duckdb: DuckDBConfig = field(default_factory=DuckDBConfig)


@dataclass
class UIConfig:
    """UI configuration settings."""

    window_width: int = 1200
    window_height: int = 800
    table_page_size: int = 100
    search_debounce_ms: int = 300
    remember_filters: bool = True


@dataclass
class ExportConfig:
    """Export configuration settings."""

    default_format: str = "csv"
    csv_delimiter: str = ","
    csv_quote_char: str = '"'
    batch_size: int = 10000
    max_export_records: int = 1000000


@dataclass
class LoggingConfig:
    """Logging configuration settings."""

    enabled: bool = True
    level: str = "INFO"
    file_path: str = "logs/mr_explore.log"
    max_file_size_mb: int = 10
    backup_count: int = 3


@dataclass
class PerformanceConfig:
    """Performance configuration settings."""

    enable_cache: bool = True
    cache_ttl_seconds: int = 300
    max_cache_size_mb: int = 500
    enable_compression: bool = True


@dataclass
class AppConfig:
    """Main application configuration."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)


class ConfigManager:
    """
    Manages application configuration with file-based persistence
    and environment variable overrides.
    """

    def __init__(self, config_path: Optional[str | Path] = None):
        """
        Initialize configuration manager.

        Args:
            config_path: Optional path to config file. If None, uses default locations.
        """
        self.config_path = self._find_config_path(config_path)
        self._config: Optional[AppConfig] = None
        self._config_dict: Optional[Dict[str, Any]] = None

    def _find_config_path(self, config_path: Optional[str | Path]) -> Path:
        """Find configuration file in standard locations."""
        if config_path:
            path = Path(config_path)
            if path.exists():
                return path
            # If specified path doesn't exist, still return it (will be created on save)

        # Check standard locations
        candidates = [
            Path("config.yaml"),
            Path("mr_explore.yaml"),
            Path.home() / ".mr-explore" / "config.yaml",
            Path.home() / ".config" / "mr-explore" / "config.yaml",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Default to config.yaml in current directory
        return Path("config.yaml")

    def load(self) -> AppConfig:
        """
        Load configuration from file.

        Returns:
            AppConfig instance with loaded or default values
        """
        if self._config is not None:
            return self._config

        # Load from file if exists
        if self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self._config_dict = yaml.safe_load(f) or {}

                # Merge with defaults
                config_dict = self._merge_with_defaults(self._config_dict)

                # Convert to AppConfig
                self._config = self._dict_to_config(config_dict)
            except Exception as e:
                # Log error but use defaults - avoid exposing sensitive paths
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Failed to load config, using defaults: {type(e).__name__}"
                )
                self._config = AppConfig()
        else:
            # Use defaults
            self._config = AppConfig()

        # Apply environment variable overrides
        self._apply_env_overrides()

        return self._config

    def _merge_with_defaults(self, user_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge user config with defaults, preserving defaults for missing keys."""
        merged = DEFAULT_CONFIG.copy()

        for section, values in user_config.items():
            if section not in merged:
                merged[section] = {}
            merged[section].update(values)

        return merged

    def _dict_to_config(self, config_dict: Dict[str, Any]) -> AppConfig:
        """Convert dictionary to AppConfig instance."""
        db_dict = config_dict.get("database", {})
        ui_dict = config_dict.get("ui", {})
        export_dict = config_dict.get("export", {})
        logging_dict = config_dict.get("logging", {})
        perf_dict = config_dict.get("performance", {})

        # Parse DuckDB configuration
        duckdb_dict = db_dict.get("duckdb", {})
        duckdb_config = DuckDBConfig(
            memory_limit=duckdb_dict.get("memory_limit", "4GB"),
            threads=duckdb_dict.get("threads", 8),
            enable_object_cache=duckdb_dict.get("enable_object_cache", True),
            max_memory=duckdb_dict.get("max_memory", "4GB"),
            temp_directory=duckdb_dict.get("temp_directory"),
            default_order=duckdb_dict.get("default_order", "ASC"),
        )

        return AppConfig(
            database=DatabaseConfig(
                default_type=db_dict.get("default_type", "sqlite"),
                sqlite_path=db_dict.get("sqlite_path", "data/mr_explore.db"),
                duckdb_pack_path=db_dict.get("duckdb_pack_path", "data/packs/main"),
                connection_pool_size=db_dict.get("connection_pool_size", 5),
                connection_timeout=db_dict.get("connection_timeout", 30),
                duckdb=duckdb_config,
            ),
            ui=UIConfig(
                window_width=ui_dict.get("window_width", 1200),
                window_height=ui_dict.get("window_height", 800),
                table_page_size=ui_dict.get("table_page_size", 100),
                search_debounce_ms=ui_dict.get("search_debounce_ms", 300),
                remember_filters=ui_dict.get("remember_filters", True),
            ),
            export=ExportConfig(
                default_format=export_dict.get("default_format", "csv"),
                csv_delimiter=export_dict.get("csv_delimiter", ","),
                csv_quote_char=export_dict.get("csv_quote_char", '"'),
                batch_size=export_dict.get("batch_size", 10000),
                max_export_records=export_dict.get("max_export_records", 1000000),
            ),
            logging=LoggingConfig(
                enabled=logging_dict.get("enabled", True),
                level=logging_dict.get("level", "INFO"),
                file_path=logging_dict.get("file_path", "logs/mr_explore.log"),
                max_file_size_mb=logging_dict.get("max_file_size_mb", 10),
                backup_count=logging_dict.get("backup_count", 3),
            ),
            performance=PerformanceConfig(
                enable_cache=perf_dict.get("enable_cache", True),
                cache_ttl_seconds=perf_dict.get("cache_ttl_seconds", 300),
                max_cache_size_mb=perf_dict.get("max_cache_size_mb", 500),
                enable_compression=perf_dict.get("enable_compression", True),
            ),
        )

    def _apply_env_overrides(self):
        """Apply environment variable overrides to config."""
        env_mappings = {
            "MR_EXPLORE_DB_TYPE": ("database", "default_type"),
            "MR_EXPLORE_SQLITE_PATH": ("database", "sqlite_path"),
            "MR_EXPLORE_DUCKDB_PATH": ("database", "duckdb_pack_path"),
            "MR_EXPLORE_LOG_LEVEL": ("logging", "level"),
            "MR_EXPLORE_LOG_PATH": ("logging", "file_path"),
            "MR_EXPLORE_EXPORT_FORMAT": ("export", "default_format"),
            "MR_EXPLORE_WINDOW_WIDTH": ("ui", "window_width"),
            "MR_EXPLORE_WINDOW_HEIGHT": ("ui", "window_height"),
        }

        for env_var, (section, key) in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Get the appropriate config object
                config_obj = getattr(self._config, section)

                # Convert type appropriately
                current_value = getattr(config_obj, key)
                if isinstance(current_value, int):
                    value = int(value)
                elif isinstance(current_value, bool):
                    value = value.lower() in ("true", "1", "yes", "on")

                setattr(config_obj, key, value)

    def save(self, config: Optional[AppConfig] = None) -> bool:
        """
        Save configuration to file.

        Args:
            config: AppConfig to save. If None, saves current config.

        Returns:
            True if successful, False otherwise
        """
        config_to_save = config or self._config

        # Create parent directories if needed
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.dump(
                    asdict(config_to_save), f, default_flow_style=False, sort_keys=False
                )
            self._config = config_to_save
            return True
        except Exception as e:
            # Log error - avoid exposing sensitive paths
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to save config: {type(e).__name__}")
            return False

    def get(self) -> AppConfig:
        """Get current configuration."""
        if self._config is None:
            return self.load()
        return self._config

    def reset_to_defaults(self) -> AppConfig:
        """Reset configuration to default values."""
        self._config = AppConfig()
        self._config_dict = None
        return self._config

    def reload(self) -> AppConfig:
        """Reload configuration from file."""
        self._config = None
        self._config_dict = None
        return self.load()


# Global config instance
_config_manager: Optional[ConfigManager] = None


def get_config(config_path: Optional[str | Path] = None) -> AppConfig:
    """
    Get global configuration instance.

    Args:
        config_path: Optional path to config file

    Returns:
        AppConfig instance
    """
    global _config_manager

    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
        _config_manager.load()

    return _config_manager.get()


def save_config(config: AppConfig) -> bool:
    """
    Save configuration to file.

    Args:
        config: AppConfig to save

    Returns:
        True if successful
    """
    global _config_manager

    if _config_manager is None:
        _config_manager = ConfigManager()

    return _config_manager.save(config)


def reset_config() -> AppConfig:
    """
    Reset configuration to defaults.

    Returns:
        AppConfig with default values
    """
    global _config_manager

    if _config_manager is None:
        _config_manager = ConfigManager()

    return _config_manager.reset_to_defaults()
