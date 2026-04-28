"""
Connection pooling for database operations.

Provides efficient connection management for SQLite and DuckDB connections,
reducing overhead of repeated connect/disconnect cycles.
"""

import sqlite3
import duckdb
import threading
from contextlib import contextmanager
from typing import Optional, Callable, Any
from queue import Queue, Empty
from dataclasses import dataclass, field
import time
from ..logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectionWrapper:
    """Wrapper for a pooled connection with tracking."""

    connection: Any  # sqlite3.Connection or duckdb.DuckDBPyConnection
    in_use: bool = False
    last_used: float = field(default_factory=time.time)
    use_count: int = 0


class ConnectionPool:
    """
    Generic connection pool for database connections.

    Manages a pool of connections that can be reused across
    operations, reducing connection overhead.
    """

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: float = 30.0,
    ):
        """
        Initialize connection pool.

        Args:
            connection_factory: Function that creates new connections
            pool_size: Maximum number of connections to keep in pool
            max_overflow: Maximum number of additional connections to create when pool is exhausted
            timeout: Maximum seconds to wait for a connection
        """
        self.connection_factory = connection_factory
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout

        # Pool with limited size - this is the "core" pool
        self._pool: Queue = Queue(maxsize=pool_size)
        self._created_connections: int = 0
        self._overflow_connections: int = 0  # Track overflow connections
        self._lock = threading.Lock()

        # Pre-create connections up to pool_size
        for _ in range(pool_size):
            try:
                conn = self.connection_factory()
                self._pool.put(ConnectionWrapper(connection=conn))
                self._created_connections += 1
            except Exception as e:
                logger.error(f"Error creating initial connection: {e}")

    def get_connection(self) -> ConnectionWrapper:
        """
        Get a connection from the pool.

        Returns:
            ConnectionWrapper with an active connection

        Raises:
            RuntimeError if timeout is exceeded
        """
        start_time = time.time()

        while time.time() - start_time < self.timeout:
            try:
                # Try to get connection from pool without blocking
                wrapper = self._pool.get_nowait()

                # Verify connection is still valid
                if self._is_connection_valid(wrapper.connection):
                    wrapper.in_use = True
                    wrapper.use_count += 1
                    wrapper.last_used = time.time()
                    return wrapper

                # Connection is stale - try to create overflow connection
                with self._lock:
                    if self._overflow_connections < self.max_overflow:
                        new_conn = self.connection_factory()
                        new_wrapper = ConnectionWrapper(
                            connection=new_conn, in_use=True, use_count=1
                        )
                        new_wrapper.last_used = time.time()
                        self._created_connections += 1
                        self._overflow_connections += 1
                        return new_wrapper
                    # Can't create more connections, wait and retry

            except Empty:
                # Pool is empty - check if we can create overflow connection
                with self._lock:
                    if self._overflow_connections < self.max_overflow:
                        new_conn = self.connection_factory()
                        new_wrapper = ConnectionWrapper(
                            connection=new_conn, in_use=True, use_count=1
                        )
                        new_wrapper.last_used = time.time()
                        self._created_connections += 1
                        self._overflow_connections += 1
                        return new_wrapper
                # Can't create more, wait and retry

            # Wait before retrying
            time.sleep(0.1)

        raise RuntimeError(f"Connection pool timeout after {self.timeout}s")

    def return_connection(self, wrapper: ConnectionWrapper):
        """
        Return a connection to the pool.

        Args:
            wrapper: ConnectionWrapper to return
        """
        wrapper.in_use = False
        wrapper.last_used = time.time()

        # Try to put back in core pool
        try:
            self._pool.put_nowait(wrapper)
        except Exception:
            # Pool is full - check if this is an overflow connection
            with self._lock:
                if self._overflow_connections > 0:
                    # This is an overflow connection, close it and decrement counter
                    self._close_connection(wrapper.connection)
                    self._overflow_connections -= 1
                    self._created_connections -= 1
                else:
                    # Core pool is full due to bug - close connection
                    logger.warning("Core pool unexpectedly full, closing connection")
                    self._close_connection(wrapper.connection)
                    self._created_connections -= 1

    def _is_connection_valid(self, connection: Any) -> bool:
        """Check if connection is still valid."""
        try:
            if isinstance(connection, sqlite3.Connection):
                # Execute a simple query to check if connection is alive
                connection.execute("SELECT 1").fetchone()
                return True
            elif isinstance(connection, duckdb.DuckDBPyConnection):
                # DuckDB connections are simpler
                connection.execute("SELECT 1").fetchone()
                return True
        except Exception:
            return False
        return True

    def _close_connection(self, connection: Any):
        """Close a connection properly."""
        try:
            if isinstance(connection, sqlite3.Connection):
                connection.close()
            elif isinstance(connection, duckdb.DuckDBPyConnection):
                connection.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            while not self._pool.empty():
                try:
                    wrapper = self._pool.get_nowait()
                    self._close_connection(wrapper.connection)
                except Empty:
                    break

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()

    def stats(self) -> dict[str, int]:
        """Get pool statistics."""
        with self._lock:
            in_use = 0
            available = 0

            # We can't iterate Queue directly, so estimate
            return {
                "created_connections": self._created_connections,
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
            }


@contextmanager
def pooled_connection(pool: ConnectionPool):
    """
    Context manager for using a pooled connection.

    Usage:
        with pooled_connection(pool) as conn:
            # Use conn
            pass
        # Connection automatically returned to pool
    """
    wrapper = pool.get_connection()
    try:
        yield wrapper.connection
    finally:
        pool.return_connection(wrapper)


class SQLiteConnectionPool(ConnectionPool):
    """Specialized connection pool for SQLite."""

    def __init__(
        self,
        db_path: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: float = 30.0,
        check_same_thread: bool = False,
    ):
        """
        Initialize SQLite connection pool.

        Args:
            db_path: Path to SQLite database file
            pool_size: Maximum number of connections in pool
            max_overflow: Maximum overflow connections
            timeout: Maximum seconds to wait for connection
            check_same_thread: Whether to check same thread (set to False for pool)

        NOTE: check_same_thread=False is required for connection pools because
        connections are created in one thread and used in another.
        The pool's locking mechanism ensures thread safety instead.
        """

        def create_connection():
            """Create a new SQLite connection."""
            return sqlite3.connect(db_path, check_same_thread=check_same_thread)

        super().__init__(create_connection, pool_size, max_overflow, timeout)


class DuckDBConnectionPool(ConnectionPool):
    """Specialized connection pool for DuckDB."""

    # Whitelist of safe DuckDB configuration variables
    SAFE_CONFIG_KEYS = {
        "memory_limit",
        "threads",
        "max_memory",
        "temp_directory",
        "default_order",
        "enable_object_cache",
    }

    def __init__(
        self,
        database: str = ":memory:",
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: float = 30.0,
        config: Optional[dict] = None,
    ):
        """
        Initialize DuckDB connection pool.

        Args:
            database: DuckDB database path (default: in-memory)
            pool_size: Maximum number of connections in pool
            max_overflow: Maximum overflow connections
            timeout: Maximum seconds to wait for connection
            config: Optional DuckDB configuration dict (keys must be in SAFE_CONFIG_KEYS)
        """

        def create_connection():
            """Create a new DuckDB connection with optional configuration."""
            conn = duckdb.connect(database)
            if config:
                for key, value in config.items():
                    # Validate config key is in whitelist to prevent SQL injection
                    if key not in self.SAFE_CONFIG_KEYS:
                        raise ValueError(
                            f"Invalid DuckDB config key '{key}'. "
                            f"Safe keys: {sorted(self.SAFE_CONFIG_KEYS)}"
                        )
                    # Validate value is string to prevent injection
                    if not isinstance(value, (str, int, float, bool)):
                        raise ValueError(
                            f"Invalid config value type for '{key}': {type(value)}"
                        )
                    # Use validated values in query
                    # DuckDB SET syntax: SET key='value' for strings, SET key=value for numbers
                    if isinstance(value, bool):
                        conn.execute(f"SET {key}={'true' if value else 'false'}")
                    elif isinstance(value, str):
                        conn.execute(f"SET {key}='{value}'")
                    else:
                        conn.execute(f"SET {key}={value}")
            return conn

        super().__init__(create_connection, pool_size, max_overflow, timeout)


# Global pool instances
_sqlite_pool: Optional[SQLiteConnectionPool] = None
_duckdb_pool: Optional[DuckDBConnectionPool] = None


def get_sqlite_pool(
    db_path: str,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> SQLiteConnectionPool:
    """
    Get or create SQLite connection pool.

    Args:
        db_path: Path to SQLite database
        pool_size: Maximum pool size
        max_overflow: Maximum overflow connections

    Returns:
        SQLiteConnectionPool instance
    """
    global _sqlite_pool

    if _sqlite_pool is None:
        _sqlite_pool = SQLiteConnectionPool(
            db_path=db_path,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )

    return _sqlite_pool


def get_duckdb_pool(
    database: str = ":memory:",
    pool_size: int = 5,
    max_overflow: int = 10,
    config: Optional[dict] = None,
) -> DuckDBConnectionPool:
    """
    Get or create DuckDB connection pool.

    Args:
        database: DuckDB database path
        pool_size: Maximum pool size
        max_overflow: Maximum overflow connections
        config: Optional DuckDB configuration

    Returns:
        DuckDBConnectionPool instance
    """
    global _duckdb_pool

    if _duckdb_pool is None:
        _duckdb_pool = DuckDBConnectionPool(
            database=database,
            pool_size=pool_size,
            max_overflow=max_overflow,
            config=config,
        )

    return _duckdb_pool


def close_all_pools():
    """Close all connection pools."""
    global _sqlite_pool, _duckdb_pool

    if _sqlite_pool is not None:
        _sqlite_pool.close_all()
        _sqlite_pool = None

    if _duckdb_pool is not None:
        _duckdb_pool.close_all()
        _duckdb_pool = None
