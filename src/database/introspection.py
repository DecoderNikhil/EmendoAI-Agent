"""
Database schema introspection for EmendoAI
Provides methods to list databases, tables, and schemas
"""
from typing import List, Dict, Any, Optional
import logging

from src.database.connection import db_connection

logger = logging.getLogger(__name__)


class SchemaIntrospector:
    """Handles database schema introspection"""
    
    def list_databases(self) -> List[str]:
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
            return databases
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
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
    
    def find_table_in_databases(self, table_name: str) -> List[Dict[str, Any]]:
        """Find a table across all databases (useful for ambiguity resolution)"""
        databases = self.list_databases()
        results = []
        
        for db in databases:
            try:
                tables = self.list_tables(database=db)
                if table_name.lower() in [t.lower() for t in tables]:
                    results.append({
                        "database": db,
                        "table": table_name
                    })
            except Exception as e:
                logger.warning(f"Could not check database {db}: {e}")
                continue
        
        return results
    
    def get_database_info(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a database"""
        return {
            "database": database or "postgres",
            "tables": self.list_tables(database)
        }


# Singleton instance
schema_introspector = SchemaIntrospector()
