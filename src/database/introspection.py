"""
Database schema introspection for EmendoAI
Provides methods to list databases, tables, and schemas
"""
from typing import List, Dict, Any, Optional
import logging
from difflib import SequenceMatcher

from src.database.connection import db_connection

logger = logging.getLogger(__name__)

# System databases to exclude
SYSTEM_DATABASES = {'template0', 'template1', 'postgres'}


class SchemaIntrospector:
    """Handles database schema introspection"""
    
    def list_databases(self, include_system: bool = False) -> List[str]:
        """List all databases on the PostgreSQL server"""
        conn = None
        try:
            conn = db_connection.get_connection()
            cursor = conn.cursor()
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
        
        Args:
            name: Database name (can be partial, case-insensitive)
            
        Returns:
            Exact database name if found, None otherwise
        """
        databases = self.list_databases(include_system=True)
        name_lower = name.lower()
        
        # Exact match (case-insensitive)
        for db in databases:
            if db.lower() == name_lower:
                return db
        
        # Partial match
        for db in databases:
            if name_lower in db.lower() or db.lower() in name_lower:
                return db
        
        # Fuzzy match
        best_match = None
        best_ratio = 0.0
        
        for db in databases:
            ratio = SequenceMatcher(None, name_lower, db.lower()).ratio()
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
        """
        Find a table by name with fuzzy matching.
        
        Args:
            table_name: Table name (can be partial)
            database: Optional database to search in
            
        Returns:
            Exact table name if found, None otherwise
        """
        tables = self.list_tables(database)
        table_lower = table_name.lower()
        
        # Exact match
        for t in tables:
            if t.lower() == table_lower:
                return t
        
        # Partial match
        for t in tables:
            if table_lower in t.lower() or t.lower() in table_lower:
                return t
        
        # Fuzzy match
        best_match = None
        best_ratio = 0.0
        
        for t in tables:
            ratio = SequenceMatcher(None, table_lower, t.lower()).ratio()
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
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
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
                    "nullable": row[2] == "YES",
                    "default": row[3],
                    "max_length": row[4],
                    "precision": row[5],
                    "scale": row[6]
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
        Search for a table across all databases.
        
        Args:
            table_name: Table name to search for
            
        Returns:
            List of dicts with 'database' and 'table' keys
        """
        databases = self.list_databases(include_system=True)
        results = []
        
        for db in databases:
            try:
                tables = self.list_tables(database=db)
                table_lower = table_name.lower()
                
                # Check for exact or partial match
                for t in tables:
                    if t.lower() == table_lower or table_lower in t.lower() or t.lower() in table_lower:
                        results.append({
                            "database": db,
                            "table": t
                        })
                        break  # Only one match per database
            except Exception as e:
                logger.warning(f"Could not check database {db}: {e}")
                continue
        
        return results
    
    def find_table_in_databases(self, table_name: str) -> List[Dict[str, Any]]:
        """Find a table across all databases (alias for backward compatibility)"""
        return self.find_table_across_all_databases(table_name)
    
    def get_database_info(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a database"""
        return {
            "database": database or "postgres",
            "tables": self.list_tables(database)
        }


# Singleton instance
schema_introspector = SchemaIntrospector()
