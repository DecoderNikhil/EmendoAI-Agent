"""
Query executor for EmendoAI
Handles SQL execution with intelligent error handling using context-aware regeneration
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
        if "null value" in error_lower and "not null" in error_lower:
            return "NOT_NULL_VIOLATION"
        
        return "UNKNOWN_ERROR"
    
    def _extract_relation_name(self) -> Optional[str]:
        """Extract table/view name from 'relation does not exist' error"""
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
        """Extract column name from 'column does not exist' error"""
        match = re.search(r'column\s+"([^"]+)"', self.error_message, re.IGNORECASE)
        if match:
            return match.group(1)
        return None


class TableIntrospector:
    """Handles table and schema introspection"""
    
    @staticmethod
    def get_tables(database: Optional[str] = None) -> List[str]:
        """Get list of available tables in database"""
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
            logger.error(f"Failed to get available tables: {e}")
            return []
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    @staticmethod
    def get_table_schema(table_name: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get schema for a specific table"""
        conn = None
        try:
            conn = db_connection.get_connection(database)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position;
            """, (table_name,))
            
            return [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3]
                }
                for row in cursor.fetchall()
            ]
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            return []
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    @staticmethod
    def find_similar_tables(table_name: str, available_tables: List[str]) -> Optional[str]:
        """Find table name most similar using SequenceMatcher"""
        if not available_tables:
            return None
            
        table_lower = table_name.lower()
        best_match = None
        best_ratio = 0.0
        
        for t in available_tables:
            # Calculate similarity ratio
            ratio = SequenceMatcher(None, table_lower, t.lower()).ratio()
            
            # Also check for substring matches
            if table_lower in t.lower() or t.lower() in table_lower:
                ratio = max(ratio, 0.8)
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = t
        
        # Only return if similarity is above threshold
        return best_match if best_ratio >= 0.5 else None


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
        """
        Regenerate SQL using context-aware approach.
        
        Returns:
            Valid SQL string or None if regeneration fails
        """
        try:
            # Step A: Introspect available tables and schemas
            available_tables = self.introspector.get_tables(database)
            
            # Build context for regeneration
            context = self._build_context(
                user_query=user_query,
                failed_sql=failed_sql,
                error=error,
                available_tables=available_tables,
                database=database
            )
            
            # Step B: Generate new SQL
            response = self.llm.generate(context)
            
            # Step C: Validate output
            validated_sql = self._validate_sql_output(response)
            
            if validated_sql:
                logger.info(f"SQL regenerated successfully: {validated_sql[:50]}...")
                return validated_sql
            
            return None
            
        except Exception as e:
            logger.error(f"SQL regeneration failed: {e}")
            return None
    
    def _build_context(
        self,
        user_query: str,
        failed_sql: str,
        error: ExecutionError,
        available_tables: List[str],
        database: Optional[str]
    ) -> str:
        """Build comprehensive context for SQL regeneration"""
        
        # Format available tables
        tables_str = ", ".join(available_tables) if available_tables else "No tables found"
        
        # Get schemas for relevant tables if it's a relation error
        schema_info = ""
        if error.error_type == "RELATION_NOT_EXISTS" and error.relation_name:
            # Try to get schema for similar tables
            similar = TableIntrospector.find_similar_tables(error.relation_name, available_tables)
            if similar:
                schema = self.introspector.get_table_schema(similar, database)
                if schema:
                    cols = [f"{c['name']} ({c['type']})" for c in schema]
                    schema_info = f"\n\nSchema for '{similar}' (possible match): {', '.join(cols)}"
        
        db_name = database or db_connection.get_active_database() or "current"
        
        prompt = f"""You are a PostgreSQL expert. Generate a new SQL query from scratch based on the user's original request.

ORIGINAL USER REQUEST:
{user_query}

PREVIOUS FAILED SQL:
{failed_sql}

ERROR MESSAGE:
{error.error_message}
Error Type: {error.error_type}

DATABASE: {db_name}

AVAILABLE TABLES:
{tables_str}
{schema_info}

INSTRUCTIONS:
1. Generate a new SQL query from SCRATCH based on the original user request
2. Use the available tables to construct the query
3. If the original table doesn't exist, try similar table names
4. Return ONLY valid SQL - no explanations, no comments, no markdown
5. Single statement only
6. Use proper PostgreSQL syntax

SQL QUERY:"""
        
        return prompt
    
    def _validate_sql_output(self, response: str) -> Optional[str]:
        """
        Validate that the LLM output is actually SQL.
        
        Returns:
            Valid SQL string or None if invalid
        """
        if not response:
            return None
        
        # Clean the response
        sql = response.strip()
        
        # Remove markdown code blocks
        if "```" in sql:
            match = re.search(r'```(?:sql)?\s*(.*?)\s*```', sql, re.DOTALL)
            if match:
                sql = match.group(1).strip()
        
        # Check for SQL keywords at start
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'WITH']
        sql_upper = sql.upper().strip()
        
        has_keyword = any(sql_upper.startswith(kw) for kw in sql_keywords)
        if not has_keyword:
            logger.warning(f"LLM output does not start with SQL keyword: {sql[:100]}")
            return None
        
        # Check for natural language sentences (heuristics)
        # If it contains multiple sentences with periods, likely not pure SQL
        sentences = sql.split('.')
        if len(sentences) > 3:
            # More than 3 sentences likely contains explanations
            # But allow for simple queries
            first_part = sentences[0].lower()
            if not any(first_part.startswith(kw.lower()) for kw in sql_keywords):
                logger.warning(f"LLM output appears to contain natural language: {sql[:100]}")
                return None
        
        # Check for common non-SQL patterns
        bad_patterns = [
            r'^here\'s',
            r'^sure,',
            r'^of course',
            r'^the query',
            r'^based on',
            r'^to fix',
        ]
        for pattern in bad_patterns:
            if re.match(pattern, sql_lower := sql.lower()):
                logger.warning(f"LLM output appears to be natural language: {sql[:100]}")
                return None
        
        # Validate length - must be substantial
        if len(sql) < 10:
            return None
        
        return sql


class QueryExecutor:
    """Handles SQL query execution with intelligent context-aware regeneration"""
    
    def __init__(self):
        self.enable_intelligent_retry = settings.ENABLE_INTELLIGENT_RETRY
        self.suggest_available_tables = settings.SUGGEST_AVAILABLE_TABLES
        self.log_sql_repairs = settings.LOG_SQL_REPAIRS
        self.max_regeneration_attempts = 2  # Max 2 regeneration tries
    
    def execute_query(
        self, 
        sql: str, 
        database: Optional[str] = None,
        params: Optional[Tuple] = None,
        with_retry: bool = True
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Execute a SQL query and return results"""
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
            error = ExecutionError(e, sql)
            logger.error(f"Query execution failed: {error.error_type} - {error.error_message}")
            raise
            
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def execute_with_intelligent_retry(
        self,
        sql: str,
        database: Optional[str] = None,
        llm_client: Optional[Any] = None,
        user_query: str = None,
        max_retries: int = None
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[List[str]], str]:
        """
        Execute SQL with context-aware regeneration on failure.
        
        Args:
            sql: SQL query to execute
            database: Target database
            llm_client: LLM client for SQL regeneration
            user_query: Original natural language query (for context)
            max_retries: Max regeneration attempts
            
        Returns:
            Tuple of (results, columns, final_sql_or_error)
        """
        if max_retries is None:
            max_retries = self.max_regeneration_attempts
        
        # Use the user query for context if not provided
        original_user_query = user_query or "Generate SQL for the user's request"
        
        current_sql = sql
        target_db = database or db_connection.get_active_database()
        
        # First attempt - try executing original SQL
        try:
            results, columns = self.execute_query(current_sql, target_db)
            return results, columns, current_sql
        except Exception as initial_error:
            initial_execution_error = ExecutionError(initial_error, current_sql)
            
            # Check if error is retryable
            if not self._is_retryable_error(initial_execution_error):
                return None, None, self._format_error_message(initial_execution_error, target_db)
        
        # Regeneration loop
        for attempt in range(max_retries):
            logger.info(f"Regeneration attempt {attempt + 1}")
            
            # Step A: Get available tables for context
            available_tables = []
            if self.suggest_available_tables:
                available_tables = TableIntrospector.get_tables(target_db)
            
            # Step B: Regenerate SQL using context
            if not llm_client:
                return None, None, self._format_error_message(initial_execution_error, target_db, available_tables)
            
            regenerator = SQLRegenerator(llm_client)
            
            # Capture original error info
            error = initial_execution_error if attempt == 0 else ExecutionError(
                Exception("Regenerated SQL still failed"), current_sql
            )
            
            new_sql = regenerator.regenerate_sql(
                user_query=original_user_query,
                failed_sql=current_sql,
                error=error,
                database=target_db
            )
            
            if not new_sql:
                # Regeneration failed - return error with available tables
                return None, None, self._format_error_message(
                    initial_execution_error, 
                    target_db, 
                    available_tables,
                    user_query=original_user_query
                )
            
            # Validate regenerated SQL
            if not self._validate_before_execution(new_sql):
                return None, None, self._format_error_message(
                    initial_execution_error,
                    target_db,
                    available_tables,
                    user_query=original_user_query,
                    reason="Regenerated SQL failed validation"
                )
            
            current_sql = new_sql
            
            # Try executing regenerated SQL
            try:
                results, columns = self.execute_query(current_sql, target_db)
                
                if self.log_sql_repairs:
                    logger.info(f"SQL regenerated: {sql[:50]}... -> {current_sql[:50]}...")
                
                return results, columns, current_sql
                
            except Exception as exec_error:
                error = ExecutionError(exec_error, current_sql)
                logger.info(f"Regenerated SQL failed: {error.error_type}")
                
                # Update error for next iteration
                initial_execution_error = error
                
                # If not retryable, stop
                if not self._is_retryable_error(error):
                    return None, None, self._format_error_message(error, target_db, available_tables)
        
        # Max retries exceeded
        return None, None, self._format_error_message(
            initial_execution_error,
            target_db,
            available_tables,
            user_query=original_user_query,
            reason="Max regeneration attempts exceeded"
        )
    
    def _validate_before_execution(self, sql: str) -> bool:
        """Validate SQL before attempting execution"""
        if not sql:
            return False
        
        sql_upper = sql.strip().upper()
        
        # Must start with SQL keyword
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'WITH']
        if not any(sql_upper.startswith(kw) for kw in sql_keywords):
            return False
        
        # Use sqlglot for validation
        try:
            import sqlglot
            parsed = sqlglot.parse(sql, dialect="postgres")
            return parsed is not None and len(parsed) > 0
        except:
            # If sqlglot fails, do basic check
            return len(sql) > 10
    
    def _is_retryable_error(self, error: ExecutionError) -> bool:
        """Check if error type allows regeneration"""
        retryable_types = ["RELATION_NOT_EXISTS", "COLUMN_NOT_EXISTS", "SYNTAX_ERROR"]
        return error.error_type in retryable_types
    
    def _format_error_message(
        self,
        error: ExecutionError,
        database: Optional[str],
        available_tables: List[str] = None,
        user_query: str = None,
        reason: str = None
    ) -> str:
        """Format comprehensive error message with context"""
        db_name = database or db_connection.get_active_database() or "current"
        
        msg_parts = []
        
        # Add reason if provided
        if reason:
            msg_parts.append(f"Error: {reason}")
        
        # Add specific error information
        if error.error_type == "RELATION_NOT_EXISTS":
            table_name = error.relation_name or "unknown"
            msg_parts.append(f"Table '{table_name}' does not exist in database '{db_name}'.")
            
            # Find similar tables
            if available_tables:
                similar = TableIntrospector.find_similar_tables(table_name, available_tables)
                if similar and similar.lower() != table_name.lower():
                    msg_parts.append(f"Did you mean '{similar}'?")
                msg_parts.append(f"\nAvailable tables: {', '.join(available_tables[:10])}")
                
        elif error.error_type == "COLUMN_NOT_EXISTS":
            col_name = error.column_name or "unknown"
            table_from_sql = self._extract_table_from_sql(error.sql)
            msg_parts.append(f"Column '{col_name}' not found.")
            if table_from_sql:
                msg_parts.append(f"In table '{table_from_sql}'.")
                
        else:
            msg_parts.append(error.error_message)
        
        return "\n".join(msg_parts)
    
    def _extract_table_from_sql(self, sql: str) -> Optional[str]:
        """Extract table name from SQL query"""
        # Simple extraction - look for FROM or UPDATE
        patterns = [
            r'FROM\s+(\w+)',
            r'UPDATE\s+(\w+)',
            r'INTO\s+(\w+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
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
