"""
Query executor for EmendoAI
Handles SQL execution with intelligent error handling
"""
from typing import List, Dict, Any, Optional, Tuple
import logging
import re
from difflib import SequenceMatcher

from config import settings
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


class ExecutionError:
    """Structured error information from SQL execution"""
    
    def __init__(self, original_error: Exception, sql: str):
        self.original_error = original_error
        self.sql = sql
        self.error_message = str(original_error)
        self.error_type = self._classify_error()
        self.relation_name = self._extract_relation_name()
        self.column_name = self._extract_column_name()
    
    def _classify_error(self) -> str:
        """Classify the type of PostgreSQL error"""
        error_lower = self.error_message.lower()
        
        if "relation" in error_lower and "does not exist" in error_lower:
            return "RELATION_NOT_EXISTS"
        if "column" in error_lower and "does not exist" in error_lower:
            return "COLUMN_NOT_EXISTS"
        if "syntax error" in error_lower or "at or near" in error_lower:
            return "SYNTAX_ERROR"
        if "permission denied" in error_lower:
            return "PERMISSION_DENIED"
        if "duplicate key" in error_lower:
            return "DUPLICATE_KEY"
        
        return "UNKNOWN_ERROR"
    
    def _extract_relation_name(self) -> Optional[str]:
        """Extract table/view name from error"""
        patterns = [
            r'relation\s+"([^"]+)"',
            r'table\s+"([^"]+)"',
            r'view\s+"([^"]+)"',
        ]
        for pattern in patterns:
            match = re.search(pattern, self.error_message, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    def _extract_column_name(self) -> Optional[str]:
        """Extract column name from error"""
        match = re.search(r'column\s+"([^"]+)"', self.error_message, re.IGNORECASE)
        if match:
            return match.group(1)
        return None


class TableIntrospector:
    """Handles table and schema introspection"""
    
    @staticmethod
    def get_tables(database: Optional[str] = None) -> List[str]:
        """Get list of available tables"""
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
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    @staticmethod
    def find_similar_tables(table_name: str, available_tables: List[str]) -> Optional[str]:
        """Find similar table name"""
        if not available_tables:
            return None
        
        table_lower = table_name.lower()
        best_match = None
        best_ratio = 0.0
        
        for t in available_tables:
            ratio = SequenceMatcher(None, table_lower, t.lower()).ratio()
            if table_lower in t.lower() or t.lower() in table_lower:
                ratio = max(ratio, 0.8)
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = t
        
        return best_match if best_ratio >= 0.5 else None


# System/catalog tables that exist in PostgreSQL but not in user databases
SYSTEM_TABLES = {
    'pg_database', 'pg_class', 'pg_attribute', 'pg_proc',
    'pg_type', 'pg_constraint', 'pg_index', 'pg_tables',
    'pg_views', 'pg_matviews', 'pg_sequence', 'pg_user',
    'pg_roles', 'pg_settings', 'pg_stat_activity'
}


class SQLValidator:
    """Validates SQL before execution"""
    
    @staticmethod
    def extract_table_names(sql: str) -> List[str]:
        """Extract table names from SQL using sqlglot"""
        try:
            import sqlglot
            parsed = sqlglot.parse(sql, dialect="postgres")
            if parsed:
                tables = parsed[0].find_all(sqlglot.exp.Table)
                return [table.name for table in tables]
        except Exception as e:
            logger.warning(f"Failed to extract tables: {e}")
        return []
    
    @staticmethod
    def is_system_query(sql: str) -> bool:
        """Check if SQL queries system tables/views (like pg_database, information_schema)"""
        sql_upper = sql.strip().upper()
        
        # Check for pg_database system catalog
        if 'PG_DATABASE' in sql_upper:
            return True
        if sql_upper.startswith('SELECT DATNAME'):
            return True
        
        # Check for information_schema queries (metadata views)
        if 'INFORMATION_SCHEMA' in sql_upper:
            return True
        
        return False
    
    @staticmethod
    def validate_sql_structure(sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL structure - must start with valid keyword"""
        sql_upper = sql.strip().upper()
        
        valid_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'WITH']
        
        for kw in valid_keywords:
            if sql_upper.startswith(kw):
                return True, None
        
        return False, f"SQL must start with one of: {', '.join(valid_keywords)}"
    
    @staticmethod
    def is_natural_language(text: str) -> bool:
        """Check if text appears to be natural language rather than SQL"""
        # Check for common natural language patterns
        nl_patterns = [
            r"^here's",
            r"^sure,",
            r"^of course",
            r"^the query",
            r"^based on",
            r"^to fix",
            r"^i can't",
            r"^i cannot",
            r"^i'm sorry",
        ]
        
        text_lower = text.lower().strip()
        for pattern in nl_patterns:
            if re.match(pattern, text_lower):
                return True
        
        # If it has many sentences, likely natural language
        sentences = text.split('.')
        if len(sentences) > 3 and not text_upper.startswith('SELECT'):
            return True
        
        return False


class SQLRegenerator:
    """Handles context-aware SQL regeneration"""
    
    def __init__(self, llm_client: Any):
        self.llm = llm_client
        self.introspector = TableIntrospector()
    
    def regenerate_sql(
        self,
        user_query: str,
        failed_sql: str,
        error: ExecutionError,
        database: Optional[str] = None
    ) -> Optional[str]:
        """Regenerate SQL using context"""
        try:
            # Get available tables
            available_tables = self.introspector.get_tables(database)
            tables_str = ", ".join(available_tables) if available_tables else "No tables found"
            
            db_name = database or db_connection.get_active_database() or "current"
            
            prompt = f"""Generate a new SQL query from scratch.

ORIGINAL USER REQUEST:
{user_query}

FAILED SQL:
{failed_sql}

ERROR:
{error.error_message}

DATABASE: {db_name}
AVAILABLE TABLES:
{tables_str}

INSTRUCTIONS:
1. Generate a new SQL query from SCRATCH based on the original request
2. Use available tables only
3. Return ONLY valid SQL - no explanations, no comments, no markdown
4. Single statement only
5. Use proper PostgreSQL syntax

SQL QUERY:"""
            
            response = self.llm.generate(prompt)
            
            # Validate the output
            validated_sql = self._validate_output(response)
            
            if validated_sql:
                logger.info(f"SQL regenerated: {validated_sql[:50]}...")
                return validated_sql
            
            return None
            
        except Exception as e:
            logger.error(f"SQL regeneration failed: {e}")
            return None
    
    def _validate_output(self, response: str) -> Optional[str]:
        """Validate regenerated SQL - PART 5: Strict validation"""
        if not response:
            return None
        
        sql = response.strip()
        
        # Remove markdown code blocks
        if "```" in sql:
            match = re.search(r'```(?:sql)?\s*(.*?)\s*```', sql, re.DOTALL)
            if match:
                sql = match.group(1).strip()
        
        # Check for natural language - ABORT if found
        if SQLValidator.is_natural_language(sql):
            logger.warning(f"Regeneration output appears to be natural language: {sql[:100]}")
            return None
        
        # Must start with valid SQL keyword
        valid, error = SQLValidator.validate_sql_structure(sql)
        if not valid:
            logger.warning(f"Regeneration output invalid: {error}")
            return None
        
        # Try sqlglot validation
        try:
            import sqlglot
            parsed = sqlglot.parse(sql, dialect="postgres")
            if not parsed:
                return None
        except:
            pass
        
        if len(sql) < 10:
            return None
        
        return sql


class QueryExecutor:
    """Handles SQL query execution"""
    
    def __init__(self):
        self.enable_intelligent_retry = settings.ENABLE_INTELLIGENT_RETRY
        self.max_regeneration_attempts = 2
        self.introspector = TableIntrospector()
    
    def execute_query(
        self, 
        sql: str, 
        database: Optional[str] = None,
        params: Optional[Tuple] = None
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Execute a SQL query"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            results = [dict(zip(column_names, row)) for row in rows]
            
            if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE")):
                conn.commit()
            
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results, column_names
            
        except Exception as e:
            if conn:
                conn.rollback()
            raise
            
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def verify_tables_exist(self, sql: str, database: str) -> Tuple[bool, Optional[str]]:
        """PART 4: Verify tables exist before execution"""
        # Skip verification for system queries (like pg_database)
        if SQLValidator.is_system_query(sql):
            return True, None
        
        table_names = SQLValidator.extract_table_names(sql)
        
        if not table_names:
            return True, None  # No tables to verify
        
        available_tables = self.introspector.get_tables(database)
        available_lower = {t.lower() for t in available_tables}
        
        missing = []
        for table in table_names:
            if table.lower() not in available_lower:
                # Try fuzzy match
                similar = TableIntrospector.find_similar_tables(table, available_tables)
                if similar:
                    missing.append(f"{table} (did you mean '{similar}'?)")
                else:
                    missing.append(table)
        
        if missing:
            return False, f"Tables not found in database '{database}': {', '.join(missing)}"
        
        return True, None
    
    def execute_with_intelligent_retry(
        self,
        sql: str,
        database: Optional[str] = None,
        llm_client: Optional[Any] = None,
        user_query: str = None,
        max_retries: int = None
    ) -> Tuple[Optional[List[Dict]], Optional[List[str]], str]:
        """Execute SQL with intelligent handling"""
        if max_retries is None:
            max_retries = self.max_regeneration_attempts
        
        original_user_query = user_query or "Generate SQL for the user's request"
        target_db = database or db_connection.get_active_database()
        
        logger.info(f"Executing on database: {target_db}")
        
        # First attempt
        try:
            results, columns = self.execute_query(sql, target_db)
            return results, columns, sql
        except Exception as initial_error:
            initial_execution_error = ExecutionError(initial_error, sql)
            
            if not self._is_retryable_error(initial_execution_error):
                return None, None, self._format_error_message(initial_execution_error, target_db)
        
        # Regeneration loop
        for attempt in range(max_retries):
            logger.info(f"Regeneration attempt {attempt + 1}")
            
            if not llm_client:
                return None, None, self._format_error_message(initial_execution_error, target_db)
            
            regenerator = SQLRegenerator(llm_client)
            
            new_sql = regenerator.regenerate_sql(
                user_query=original_user_query,
                failed_sql=sql,
                error=initial_execution_error,
                database=target_db
            )
            
            if not new_sql:
                # PART 5: Abort - don't execute original SQL
                return None, None, self._format_error_message(
                    initial_execution_error, target_db,
                    reason="Could not regenerate valid SQL"
                )
            
            # Try executing regenerated SQL
            try:
                results, columns = self.execute_query(new_sql, target_db)
                logger.info(f"SQL regenerated successfully")
                return results, columns, new_sql
                
            except Exception as exec_error:
                error = ExecutionError(exec_error, new_sql)
                logger.info(f"Regenerated SQL failed: {error.error_type}")
                
                if not self._is_retryable_error(error):
                    return None, None, self._format_error_message(error, target_db)
                
                initial_execution_error = error
        
        return None, None, self._format_error_message(
            initial_execution_error, target_db,
            reason="Max regeneration attempts exceeded"
        )
    
    def _is_retryable_error(self, error: ExecutionError) -> bool:
        """Check if error allows regeneration"""
        return error.error_type in ["RELATION_NOT_EXISTS", "COLUMN_NOT_EXISTS", "SYNTAX_ERROR"]
    
    def _format_error_message(
        self,
        error: ExecutionError,
        database: str,
        reason: str = None
    ) -> str:
        """Format error message"""
        msg_parts = []
        
        if reason:
            msg_parts.append(f"Error: {reason}")
        
        if error.error_type == "RELATION_NOT_EXISTS":
            table = error.relation_name or "unknown"
            msg_parts.append(f"Table '{table}' does not exist in database '{database}'.")
        elif error.error_type == "COLUMN_NOT_EXISTS":
            col = error.column_name or "unknown"
            msg_parts.append(f"Column '{col}' not found.")
        else:
            msg_parts.append(error.error_message)
        
        return "\n".join(msg_parts)
    
    def get_affected_rows(self, sql: str, database: Optional[str] = None) -> int:
        """Get rows affected by query"""
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
            raise
        finally:
            if conn:
                db_connection.release_connection(conn)


# Singleton instance
query_executor = QueryExecutor()

# For backward compatibility
introspector = TableIntrospector()
