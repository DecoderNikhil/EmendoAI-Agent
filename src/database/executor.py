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
    def get_table_schema(table_name: str,
        self, 
        sql: str, 
        database: Optional[str] = None,
        params: Optional[Tuple] = None,
        with_retry: bool = True
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query and return results
        
        Args:
            sql: SQL query to execute
            database: Target database (optional)
            params: Query parameters (optional)
            with_retry: Whether to use intelligent retry logic
            
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
            
            # Create structured error
            error = ExecutionError(e, sql)
            logger.error(f"Query execution failed: {error.error_type} - {error.error_message}")
            
            # Re-raise to let caller handle retry logic
            raise
            
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def execute_with_intelligent_retry(
        self,
        sql: str,
        database: Optional[str] = None,
        llm_client: Optional[Any] = None,
        max_retries: int = None
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[List[str]], str]:
        """
        Execute SQL with intelligent error handling and LLM-based repair.
        
        Args:
            sql: SQL query to execute
            database: Target database
            llm_client: LLM client for SQL repair
            max_retries: Maximum retry attempts
            
        Returns:
            Tuple of (results, columns, final_sql_or_error_message)
        """
        if max_retries is None:
            max_retries = settings.MAX_EXECUTION_RETRIES
        
        current_sql = sql
        available_tables: List[str] = []
        
        for attempt in range(max_retries):
            try:
                # Determine target database
                target_db = database or db_connection.get_active_database()
                
                # Execute the query
                results, columns = self.execute_query(current_sql, target_db, with_retry=False)
                return results, columns, current_sql
                
            except Exception as e:
                error = ExecutionError(e, current_sql)
                
                logger.info(f"Attempt {attempt + 1} failed with: {error.error_type}")
                
                # Check if this error type is retryable
                if not self.enable_intelligent_retry or not self._is_retryable_error(error):
                    # Return error message
                    return None, None, self._format_error_message(error, available_tables)
                
                # If relation not exists, get available tables for suggestion
                if error.error_type == "RELATION_NOT_EXISTS" and self.suggest_available_tables:
                    try:
                        target_db = database or db_connection.get_active_database()
                        available_tables = self._get_available_tables(target_db)
                    except:
                        pass
                
                # Try to repair SQL using LLM
                if llm_client and attempt < max_retries - 1:
                    repair_result = self._repair_sql(
                        current_sql, 
                        error, 
                        llm_client,
                        available_tables
                    )
                    
                    if repair_result:
                        current_sql = repair_result
                        
                        if self.log_sql_repairs:
                            logger.info(f"SQL repaired: {sql[:50]}... -> {current_sql[:50]}...")
                        
                        continue  # Retry with repaired SQL
                
                # No more retries or no LLM client
                if attempt == max_retries - 1:
                    return None, None, self._format_error_message(error, available_tables)
        
        return None, None, "Max retries exceeded"
    
    def _is_retryable_error(self, error: ExecutionError) -> bool:
        """Check if error type should trigger retry with repair"""
        retryable_types = [
            "RELATION_NOT_EXISTS",
            "COLUMN_NOT_EXISTS", 
            "SYNTAX_ERROR"
        ]
        return error.error_type in retryable_types
    
    def _repair_sql(
        self,
        original_sql: str,
        error: ExecutionError,
        llm_client: Any,
        available_tables: List[str] = None
    ) -> Optional[str]:
        """Use LLM to repair SQL based on error message"""
        try:
            # Build repair prompt
            tables_info = ""
            if available_tables:
                tables_info = f"\nAvailable tables in database: {', '.join(available_tables)}"
            
            prompt = f"""You are a PostgreSQL expert. The following SQL query failed with an error.

Original SQL:
```sql
{original_sql}
```

Error: {error.error_message}
Error Type: {error.error_type}
{tables_info}

Instructions:
1. Analyze the error and the SQL query
2. Fix the SQL to resolve the error
3. Only return the corrected SQL query, nothing else
4. If the table doesn't exist but there's a similar available table, use that table name
5. Use proper PostgreSQL syntax

Corrected SQL:"""
            
            # Get LLM response
            response = llm_client.generate(prompt)
            
            # Extract SQL from response
            if hasattr(self, '_extract_sql_from_response'):
                repaired_sql = self._extract_sql_from_response(response)
            else:
                # Simple extraction
                repaired_sql = response.strip()
                # Remove markdown code blocks if present
                if "```" in repaired_sql:
                    match = re.search(r'```(?:sql)?\s*(.*?)\s*```', repaired_sql, re.DOTALL)
                    if match:
                        repaired_sql = match.group(1).strip()
            
            # Validate repaired SQL looks reasonable
            if repaired_sql and len(repaired_sql) > 10:
                return repaired_sql
            
        except Exception as e:
            logger.error(f"Failed to repair SQL: {e}")
        
        return None
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL from LLM response"""
        # Try code blocks first
        match = re.search(r'```(?:sql)?\s*(.*?)\s*```', response, re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # Try to find SQL starting with keyword
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']
        for keyword in sql_keywords:
            pattern = rf'\b{keyword}\b'
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                return response[match.start():].strip()
        
        return response.strip()
    
    def _get_available_tables(self, database: Optional[str] = None) -> List[str]:
        """Get list of available tables in database"""
        try:
            conn = None
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
            logger.error(f"Failed to get available tables: {e}")
            return []
        finally:
            if conn:
                db_connection.release_connection(conn)
    
    def _format_error_message(
        self, 
        error: ExecutionError,
        available_tables: List[str] = None
    ) -> str:
        """Format error into user-friendly message"""
        msg = error.error_message
        
        # Add relation suggestion
        if error.relation_name and available_tables:
            # Find similar table names
            similar = self._find_similar_tables(error.relation_name, available_tables)
            if similar:
                msg += f"\n\nAvailable tables: {', '.join(available_tables[:10])}"
                if similar != error.relation_name:
                    msg += f"\nDid you mean '{similar}'?"
        
        # Add column suggestion
        if error.column_name and error.get_suggestion():
            msg += f"\n\n{error.get_suggestion()}"
        
        return msg
    
    def _find_similar_tables(
        self, 
        table_name: str, 
        available_tables: List[str]
    ) -> Optional[str]:
        """Find table name most similar to the requested one"""
        table_lower = table_name.lower()
        
        # Exact match (case insensitive)
        for t in available_tables:
            if t.lower() == table_lower:
                return t
        
        # Partial match
        for t in available_tables:
            if table_lower in t.lower() or t.lower() in table_lower:
                return t
        
        # Levenshtein-like: check for common typos
        # Simple approach: check first char matches and high similarity
        for t in available_tables:
            if t.lower().startswith(table_lower[0]) and len(table_lower) > 2:
                # Simple similarity check
                matches = sum(1 for a, b in zip(t.lower(), table_lower) if a == b)
                if matches >= len(table_lower) * 0.6:
                    return t
        
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
