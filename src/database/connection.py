"""
PostgreSQL connection manager for EmendoAI
"""
import psycopg2
from psycopg2 import pool
from typing import Optional, Dict, Any
import logging

from config import settings

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL connections with connection pooling"""
    
    _instance: Optional['DatabaseConnection'] = None
    _connection_pool: Optional[pool.ThreadedConnectionPool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Track active database
        self._active_database: Optional[str] = None
        # Store dedicated connections for each database
        self._dedicated_connections: Dict[str, psycopg2.extensions.connection] = {}
        
        if self._connection_pool is None:
            self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            self._connection_pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.POSTGRES_DEFAULT_DB
            )
            self._active_database = settings.POSTGRES_DEFAULT_DB
            logger.info("Database connection pool initialized")
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def get_active_database(self) -> Optional[str]:
        """Get the currently active database"""
        return self._active_database
    
    def set_active_database(self, database: Optional[str]) -> bool:
        """
        Switch to a different database.
        
        Args:
            database: The database name to switch to
            
        Returns:
            True if successful, False otherwise
        """
        if not database:
            self._active_database = settings.POSTGRES_DEFAULT_DB
            return True
            
        # Test connection to the new database first
        try:
            test_conn = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=database
            )
            test_conn.close()
            
            self._active_database = database
            logger.info(f"Switched to database: {database}")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to switch to database {database}: {e}")
            return False
    
    def get_connection(self, database: Optional[str] = None) -> psycopg2.extensions.connection:
        """
        Get a connection from the pool or dedicated connection.
        
        Args:
            database: Optional database name. If None, uses active database.
        """
        # Determine target database
        target_db = database or self._active_database or settings.POSTGRES_DEFAULT_DB
        
        try:
            # If targeting a specific database, create a dedicated connection
            # This ensures queries run against the correct database
            if target_db and target_db != settings.POSTGRES_DEFAULT_DB:
                # Check if we already have a dedicated connection
                if target_db not in self._dedicated_connections:
                    # Create new dedicated connection
                    conn = psycopg2.connect(
                        host=settings.POSTGRES_HOST,
                        port=settings.POSTGRES_PORT,
                        user=settings.POSTGRES_USER,
                        password=settings.POSTGRES_PASSWORD,
                        database=target_db
                    )
                    self._dedicated_connections[target_db] = conn
                else:
                    conn = self._dedicated_connections[target_db]
                    # Check if connection is still valid
                    try:
                        conn.cursor().execute("SELECT 1")
                    except:
                        # Reconnect if connection is dead
                        conn = psycopg2.connect(
                            host=settings.POSTGRES_HOST,
                            port=settings.POSTGRES_PORT,
                            user=settings.POSTGRES_USER,
                            password=settings.POSTGRES_PASSWORD,
                            database=target_db
                        )
                        self._dedicated_connections[target_db] = conn
                return conn
            else:
                # Use pool connection for default database
                return self._connection_pool.getconn()
                
        except psycopg2.Error as e:
            logger.error(f"Failed to get connection: {e}")
            raise
    
    def release_connection(self, conn: psycopg2.extensions.connection):
        """Return a connection to the pool"""
        # Only release back to pool if it's from the pool
        # Dedicated connections are managed separately
        try:
            # Check if this is a pooled connection
            if self._connection_pool:
                # Try to release to pool (will fail gracefully if not from pool)
                try:
                    self._connection_pool.putconn(conn)
                except:
                    pass  # Not from pool, ignore
        except Exception as e:
            logger.error(f"Failed to release connection: {e}")
    
    def close_all(self):
        """Close all connections in the pool"""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("All database connections closed")
        
        # Close dedicated connections
        for db_name, conn in self._dedicated_connections.items():
            try:
                conn.close()
                logger.info(f"Closed dedicated connection to: {db_name}")
            except Exception as e:
                logger.error(f"Failed to close connection to {db_name}: {e}")
        
        self._dedicated_connections.clear()
    
    def test_connection(self, database: Optional[str] = None) -> bool:
        """Test if database connection works"""
        conn = None
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"PostgreSQL version: {version[0]}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
        finally:
            if conn and self._connection_pool:
                try:
                    self.release_connection(conn)
                except:
                    pass


# Singleton instance
db_connection = DatabaseConnection()
