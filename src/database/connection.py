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
            logger.info("Database connection pool initialized")
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def get_connection(self, database: Optional[str] = None) -> psycopg2.extensions.connection:
        """Get a connection from the pool"""
        try:
            if database and database != settings.POSTGRES_DEFAULT_DB:
                # Connect to specific database
                conn = psycopg2.connect(
                    host=settings.POSTGRES_HOST,
                    port=settings.POSTGRES_PORT,
                    user=settings.POSTGRES_USER,
                    password=settings.POSTGRES_PASSWORD,
                    database=database
                )
            else:
                conn = self._connection_pool.getconn()
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to get connection: {e}")
            raise
    
    def release_connection(self, conn: psycopg2.extensions.connection):
        """Return a connection to the pool"""
        try:
            self._connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release connection: {e}")
    
    def close_all(self):
        """Close all connections in the pool"""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("All database connections closed")
    
    def test_connection(self) -> bool:
        """Test if database connection works"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"PostgreSQL version: {version[0]}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
        finally:
            if conn:
                self.release_connection(conn)


# Singleton instance
db_connection = DatabaseConnection()
