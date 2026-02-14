"""
SQL Validator using sqlglot for EmendoAI
Validates SQL syntax and ensures PostgreSQL compatibility
"""
import re
import sqlglot
from sqlglot.errors import ParseError
from typing import Tuple, Optional, List
import logging

from config import settings

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates SQL queries using sqlglot"""
    
    def __init__(self):
        self.blocked_patterns = settings.BLOCKED_SQL_PATTERNS
        self.allowed_keywords = settings.ALLOWED_SQL_KEYWORDS
    
    def validate(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL syntax and security
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for blocked patterns (SQL injection protection)
        blocked_reason = self._check_blocked_patterns(sql)
        if blocked_reason:
            return False, blocked_reason
        
        # Validate SQL syntax
        try:
            # Parse the SQL - will raise ParseError if invalid
            parsed = sqlglot.parse(sql, dialect="postgres")
            
            if not parsed:
                return False, "Failed to parse SQL"
            
            # Check if the SQL operation is allowed
            sql_type = self._get_sql_type(sql)
            if sql_type not in self.allowed_keywords:
                return False, f"Operation '{sql_type}' is not allowed"
            
            return True, None
            
        except ParseError as e:
            logger.warning(f"SQL parse error: {e}")
            return False, f"Invalid SQL syntax: {str(e)}"
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False, f"Validation error: {str(e)}"
    
    def _check_blocked_patterns(self, sql: str) -> Optional[str]:
        """Check for blocked SQL injection patterns"""
        sql_upper = sql.upper()
        
        for pattern in self.blocked_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                if "DROP DATABASE" in pattern.upper():
                    return "DROP DATABASE is blocked for security reasons"
                return f"Blocked SQL pattern detected: {pattern}"
        
        return None
    
    def _get_sql_type(self, sql: str) -> Optional[str]:
        """Get the SQL operation type (SELECT, INSERT, etc.)"""
        sql_stripped = sql.strip().upper()
        
        for keyword in self.allowed_keywords:
            if sql_stripped.startswith(keyword):
                return keyword
        
        return None
    
    def to_postgres(self, sql: str) -> str:
        """
        Convert SQL to PostgreSQL dialect
        Useful if the LLM generates MySQL or other dialect SQL
        """
        try:
            parsed = sqlglot.transpile(sql, read=None, write="postgres")[0]
            return parsed
        except Exception as e:
            logger.warning(f"Failed to transpile SQL: {e}")
            return sql
    
    def extract_tables(self, sql: str) -> List[str]:
        """Extract table names from a SQL query"""
        try:
            parsed = sqlglot.parse(sql, dialect="postgres")
            if parsed:
                tables = parsed[0].find_all(sqlglot.exp.Table)
                return [table.name for table in tables]
        except Exception as e:
            logger.warning(f"Failed to extract tables: {e}")
        return []
    
    def is_read_only(self, sql: str) -> bool:
        """Check if the SQL query is read-only (SELECT)"""
        sql_type = self._get_sql_type(sql)
        return sql_type in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]


# Singleton instance
sql_validator = SQLValidator()
