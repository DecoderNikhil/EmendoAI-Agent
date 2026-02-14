"""
Database schema introspection for EmendoAI
Provides methods to list databases, tables, and schemas
"""
from typing import List, Dict, Any, Optional
import logging
import re
from difflib import SequenceMatcher

from src.database.connection import db_connection

logger = logging.getLogger(__name__)

# System databases to exclude
SYSTEM_DATABASES = {'template0', 'template1', 'postgres'}

# Cache for database -> tables mapping
_db_table_cache: Dict[str, List[str]] = {}
_cache_valid = False


def _normalize(text: str) -> str:
    """Normalize text for fuzzy matching: lowercase, remove spaces, hyphens, underscores"""
    return text.lower().replace(' ', '').replace('-', '').replace('_', '')


class SchemaIntrospector:
    """Handles database schema introspection"""
    
    def refresh_cache(self) -> None:
        """Build cached mapping of database -> tables"""
        global _db_table_cache, _cache_valid
        _db_table_cache = {}
        
        try:
            databases = self.list_databases(include_system=True)
            for db in databases:
                try:
                    tables = self.list_tables(db)
                    _db_table_cache[db] = tables
                except Exception as e:
                    logger.warning(f"Could not get tables from {db}: {e}")
                    _db_table_cache[db] = []
            _cache_valid = True
            logger.info(f"Database cache refreshed: {len(_db_table_cache)} databases")
        except Exception as e:
            logger.error(f"Failed to refresh cache: {e}")
            _cache_valid = False
    
    def get_cached_tables(self) -> Dict[str, List[str]]:
        """Get cached database -> tables mapping"""
        global _cache_valid
        if not _cache_valid:
            self.refresh_cache()
        return _db_table_cache
    
    def list_databases(self, include_system: bool = False) -> List[str]:
        """List all databases on the PostgreSQL server"""
        conn = None
        try:
            conn = db_connection.get_connection()
            cursor = conn.cursor()
            # PART 1: Standardized query - filter by datistemplate = false
            cursor.execute("""
                SELECT datname 
                FROM pg_database 
                WHERE datistemplate = false 
                ORDER BY datname;
            """)
            databases = [row[0] for row in cursor.fetchall()]
            
            # Filter out system databases by default
            if not include_system:
                databases = [db for db in databases if db.lower() not in SYSTEM_DATABASES]
            
            return databases
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def find_database(self, name: str) -> Optional[str]:
        """
        Find a database by name with fuzzy matching.
        PART 7: Fuzzy Database Matching - normalize and match
        """
        databases = self.list_databases(include_system=True)
        normalized_input = _normalize(name)
        
        # Exact match (case-insensitive, normalized)
        for db in databases:
            if _normalize(db) == normalized_input:
                return db
        
        # Partial match (normalized)
        for db in databases:
            if normalized_input in _normalize(db) or _normalize(db) in normalized_input:
                return db
        
        # Fuzzy match
        best_match = None
        best_ratio = 0.0
        
        for db in databases:
            ratio = SequenceMatcher(None, normalized_input, _normalize(db)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = db
        
        if best_ratio >= 0.5:
            return best_match
        
        return None
    
    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """List all tables in the specified database"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cursor.fetchall()]
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def find_table(self, table_name: str, database: Optional[str] = None) -> Optional[str]:
        """Find a table by name with fuzzy matching."""
        tables = self.list_tables(database)
        normalized_input = _normalize(table_name)
        
        # Exact match
        for t in tables:
            if _normalize(t) == normalized_input:
                return t
        
        # Partial match
        for t in tables:
            if normalized_input in _normalize(t) or _normalize(t) in normalized_input:
                return t
        
        # Fuzzy match
        best_match = None
        best_ratio = 0.0
        
        for t in tables:
            ratio = SequenceMatcher(None, normalized_input, _normalize(t)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = t
        
        if best_ratio >= 0.5:
            return best_match
        
        return None
    
    def get_table_schema(self, table_name: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get the schema (columns, types) of a table"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            # PART 3: Standardized schema query
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s 
                    AND table_schema = 'public'
                ORDER BY ordinal_position;
            """, (table_name,))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "max_length": row[2],
                    "nullable": row[3] == "YES",
                    "default": row[4]
                })
            return columns
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def get_table_info(self, table_name: str, database: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive info about a table including schema"""
        return {
            "table_name": table_name,
            "schema": self.get_table_schema(table_name, database)
        }
    
    def find_table_across_all_databases(self, table_name: str) -> List[Dict[str, Any]]:
        """
        PART 2: Search for a table across all databases using cache.
        Returns list of {database, table} for matches.
        """
        # Refresh cache to ensure latest data
        self.refresh_cache()
        
        results = []
        normalized_input = _normalize(table_name)
        
        for db, tables in _db_table_cache.items():
            for t in tables:
                if _normalize(t) == normalized_input:
                    results.append({"database": db, "table": t})
                    break  # One match per database
        
        return results
    
    def find_table_in_databases(self, table_name: str) -> List[Dict[str, Any]]:
        """Alias for backward compatibility"""
        return self.find_table_across_all_databases(table_name)
    
    def get_database_info(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a database"""
        return {
            "database": database or "postgres",
            "tables": self.list_tables(database)
        }


# Singleton instance
schema_introspector = SchemaIntrospector()
