"""
Safety checks for EmendoAI agent
Handles permission requests for destructive actions and bulk updates
"""
from typing import Tuple, Optional, List
import re
import logging

from config import settings

logger = logging.getLogger(__name__)


class QueryType:
    """SQL Query types for safety classification"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    TRUNCATE = "TRUNCATE"
    OTHER = "OTHER"


class SafetyChecker:
    """Handles safety checks for SQL queries"""
    
    def __init__(self):
        self.delete_threshold = settings.DELETE_ROWS_THRESHOLD
    
    def classify_query(self, sql: str) -> str:
        """Classify the SQL query type"""
        sql_upper = sql.strip().upper()
        
        for query_type in [
            QueryType.SELECT, QueryType.INSERT, QueryType.UPDATE,
            QueryType.DELETE, QueryType.CREATE, QueryType.DROP,
            QueryType.ALTER, QueryType.TRUNCATE
        ]:
            if sql_upper.startswith(query_type):
                return query_type
        
        return QueryType.OTHER
    
    def requires_permission(self, sql: str) -> Tuple[bool, str]:
        """
        Check if query requires user permission
        
        Returns:
            Tuple of (requires_permission, reason)
        """
        query_type = self.classify_query(sql)
        
        # DELETE more than threshold rows requires permission
        if query_type == QueryType.DELETE:
            # We can't know exact count until we run it
            # So we'll always ask for DELETE permission to be safe
            return True, f"DELETE query detected. This will delete rows from the table."
        
        # DROP TABLE requires permission
        if query_type == QueryType.DROP:
            return True, f"DROP {self._get_drop_target(sql)} query detected. This is a destructive action."
        
        # TRUNCATE requires permission
        if query_type == QueryType.TRUNCATE:
            return True, "TRUNCATE query detected. This will delete all rows from the table."
        
        return False, ""
    
    def _get_drop_target(self, sql: str) -> str:
        """Extract what is being dropped (TABLE, DATABASE, etc.)"""
        sql_upper = sql.strip().upper()
        
        if "TABLE" in sql_upper:
            return "TABLE"
        elif "DATABASE" in sql_upper:
            return "DATABASE"
        elif "INDEX" in sql_upper:
            return "INDEX"
        else:
            return "OBJECT"
    
    def needs_warning(self, sql: str) -> Tuple[bool, str]:
        """
        Check if query needs a warning (e.g., bulk update)
        
        Returns:
            Tuple of (needs_warning, message)
        """
        query_type = self.classify_query(sql)
        
        # UPDATE affects multiple rows - needs warning
        if query_type == QueryType.UPDATE:
            return True, "UPDATE query detected. This may affect multiple rows."
        
        return False, ""
    
    def get_update_estimate(self, sql: str, database: Optional[str] = None) -> Optional[int]:
        """
        Estimate how many rows will be affected by UPDATE/DELETE
        This would need the executor to actually check
        """
        # This is a placeholder - actual implementation would query
        # the database to get estimated row count
        return None
    
    def format_permission_message(self, sql: str) -> str:
        """Format a user-friendly permission request message"""
        query_type = self.classify_query(sql)
        
        if query_type == QueryType.DELETE:
            table_name = self._extract_table_name(sql)
            return f"You're about to DELETE rows from '{table_name}'. This action cannot be undone. Do you want to proceed?"
        
        if query_type == QueryType.DROP:
            target = self._get_drop_target(sql)
            name = self._extract_drop_name(sql)
            return f"You're about to {target} '{name}'. This is a destructive action and cannot be undone. Do you want to proceed?"
        
        if query_type == QueryType.TRUNCATE:
            table_name = self._extract_table_name(sql)
            return f"You're about to TRUNCATE '{table_name}'. All rows will be deleted. Do you want to proceed?"
        
        return "This query will modify data. Do you want to proceed?"
    
    def _extract_table_name(self, sql: str) -> str:
        """Extract table name from SQL"""
        sql_upper = sql.upper()
        
        # Pattern for DELETE FROM table_name
        match = re.search(r'FROM\s+(\w+)', sql_upper)
        if match:
            return match.group(1)
        
        # Pattern for UPDATE table_name
        match = re.search(r'UPDATE\s+(\w+)', sql_upper)
        if match:
            return match.group(1)
        
        return "unknown"
    
    def _extract_drop_name(self, sql: str) -> str:
        """Extract name from DROP statement"""
        sql_upper = sql.upper()
        
        # Pattern for DROP TABLE/DATABASE name
        match = re.search(r'DROP\s+\w+\s+(\w+)', sql_upper)
        if match:
            return match.group(1)
        
        return "unknown"
    
    def should_auto_execute(self, sql: str) -> bool:
        """
        Determine if query should be auto-executed
        (without requiring explicit permission)
        """
        query_type = self.classify_query(sql)
        
        # SELECT, INSERT, CREATE are auto-executed
        if query_type in [QueryType.SELECT, QueryType.INSERT, QueryType.CREATE]:
            return True
        
        # UPDATE requires warning but not permission
        if query_type == QueryType.UPDATE:
            return True
        
        # Everything else requires permission
        return False


# Singleton instance
safety_checker = SafetyChecker()
