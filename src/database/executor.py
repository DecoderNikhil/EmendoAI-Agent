"""
Query executor for EmendoAI
Handles SQL execution with proper error handling
"""
from typing import List, Dict, Any, Optional, Tuple
import logging

from src.database.connection import db_connection

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Handles SQL query execution"""
    
    def execute_query(
        self, 
        sql: str, 
        database: Optional[str] = None,
        params: Optional[Tuple] = None
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query and return results
        
        Returns:
            Tuple of (rows, column_names)
        """
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            
            # Execute with parameters if provided
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all results
            rows = cursor.fetchall()
            
            # Convert to list of dicts
            results = []
            for row in rows:
                results.append(dict(zip(column_names, row)))
            
            # Commit for modification queries
            if sql.strip().upper().startswith((
                "INSERT", "UPDATE", "DELETE", "CREATE", 
                "DROP", "ALTER", "TRUNCATE"
            )):
                conn.commit()
            
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results, column_names
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def execute_single(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Any:
        """Execute a query and return a single value"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def get_affected_rows(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> int:
        """Get the number of rows affected by a query"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def get_update_affected_rows(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> int:
        """Preview affected rows for UPDATE/DELETE (without committing)"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            cursor.execute(sql)
            rowcount = cursor.rowcount
            conn.rollback()  # Don't commit, just preview
            return rowcount
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)


# Singleton instance
query_executor = QueryExecutor()
