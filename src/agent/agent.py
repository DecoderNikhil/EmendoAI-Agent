"""
Main EmendoAI Agent
Ties all components together and handles the full flow
"""
from typing import Tuple, Optional, List, Dict, Any
import logging

from config import settings
from src.llm.anthropic_client import ClaudeClient
from src.llm.bedrock_client import BedrockClient
from src.llm.cli_client import CLIClaudeClient
from src.database.introspection import schema_introspector
from src.database.executor import query_executor
from src.database.connection import db_connection
from src.sql.validator import sql_validator
from src.agent.prompt_builder import prompt_builder
from src.agent.safety import safety_checker
from src.agent.response_parser import response_parser

logger = logging.getLogger(__name__)


def _create_llm_client():
    """Create the appropriate LLM client based on settings"""
    
    # Priority: CLI > Bedrock > Anthropic API
    if settings.USE_CLAUDE_CLI:
        logger.info("Using Claude CLI client")
        return CLIClaudeClient()
    
    if settings.USE_BEDROCK:
        logger.info("Using Amazon Bedrock client")
        return BedrockClient(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
    
    # Default: Use direct Anthropic API
    logger.info("Using Anthropic API client")
    return ClaudeClient(settings.ANTHROPIC_API_KEY)


class EmendoAIAgent:
    """
    Main AI Agent that converts natural language to SQL and back.
    
    Features:
    - Proper database context switching
    - Intelligent error handling with LLM-based SQL repair
    - Robust schema command parsing
    - Better UX messages for errors
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        api_key: Optional[str] = None,
        force_client: Optional[str] = None
    ):
        """
        Initialize the agent with the specified LLM client.
        
        Args:
            aws_access_key_id: AWS access key for Bedrock
            aws_secret_access_key: AWS secret key for Bedrock
            region_name: AWS region for Bedrock
            api_key: Anthropic API key for direct API
            force_client: Force a specific client ('cli', 'bedrock', 'anthropic')
        """
        
        # Determine which client to use
        if force_client == 'cli':
            self.llm = CLIClaudeClient()
        elif force_client == 'bedrock':
            self.llm = BedrockClient(
                aws_access_key_id=aws_access_key_id or settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=aws_secret_access_key or settings.AWS_SECRET_ACCESS_KEY,
                region_name=region_name or settings.AWS_REGION
            )
        elif force_client == 'anthropic':
            self.llm = ClaudeClient(api_key or settings.ANTHROPIC_API_KEY)
        else:
            # Auto-detect based on settings
            self.llm = _create_llm_client()
        
        self.introspector = schema_introspector
        self.executor = query_executor
        self.validator = sql_validator
        self.prompt_builder = prompt_builder
        self.safety = safety_checker
        self.parser = response_parser
        
        # Retry settings
        self.max_generation_retries = settings.MAX_GENERATION_RETRIES
        self.max_execution_retries = settings.MAX_EXECUTION_RETRIES
        
        # State - track active database
        self.current_database: Optional[str] = db_connection.get_active_database()
        self.conversation_history: List[Dict[str, str]] = []
        
        # Enable intelligent features
        self.enable_intelligent_retry = settings.ENABLE_INTELLIGENT_RETRY
    
    def process_query(
        self, 
        user_query: str,
        database: Optional[str] = None,
        user_approved: bool = False
    ) -> Tuple[str, Optional[str]]:
        """
        Process a natural language query with intelligent handling.
        
        Returns:
            Tuple of (response, sql_query)
            - response: Natural language response to user
            - sql_query: The SQL that was executed (None if not executed)
        """
        # First, handle special commands (schema, database switch, list commands)
        special_response = self._handle_special_command(user_query, database)
        if special_response:
            return special_response
        
        # Determine which database to use
        target_db = database or self.current_database
        
        # Step 1: Generate SQL from natural language
        sql = self._generate_sql(user_query, target_db)
        
        if not sql:
            return "I couldn't generate a SQL query from your request. Could you rephrase it?", None
        
        # Step 2: Validate SQL
        is_valid, error_msg = self.validator.validate(sql)
        
        if not is_valid:
            return f"Generated SQL is invalid: {error_msg}", sql
        
        # Step 3: Check safety
        requires_permission, _ = self.safety.requires_permission(sql)
        
        if requires_permission and not user_approved:
            # Return permission request message
            permission_msg = self.safety.format_permission_message(sql)
            return f"{permission_msg}\n\nGenerated SQL:\n```sql\n{sql}\n```", sql
        
        # Step 4: Check if UPDATE and warn about affected rows
        needs_warning, warning_msg = self.safety.needs_warning(sql)
        
        # Step 5: Execute the query with intelligent retry
        result = self._execute_with_intelligent_retry(sql, target_db)
        
        if result is None:
            return "Query execution failed. Please check the SQL and try again.", sql
        
        # Check if result contains error message instead of data
        if isinstance(result, str):
            # This is an error message
            return result, sql
        
        results, columns, rows_affected, final_sql = result
        
        # Step 6: Format response
        query_type = self.safety.classify_query(final_sql)
        
        # Add warning if applicable
        warning = f"{warning_msg}\n\n" if needs_warning else ""
        
        # Check if execution had intelligent repair
        repair_info = ""
        if final_sql != sql and self.enable_intelligent_retry:
            repair_info = f"_Note: SQL was automatically corrected_\n\n"
        
        formatted_result = self.parser.format_execution_result(
            query_type, 
            rows_affected, 
            results if query_type == "SELECT" else None
        )
        
        # Update current database if changed
        if query_type == "CREATE" or "DATABASE" in final_sql.upper():
            # Refresh database list
            pass
        
        response = f"{warning}{repair_info}{formatted_result}"
        
        return response, final_sql
    
    def _handle_special_command(
        self, 
        user_query: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[str, Optional[str]]]:
        """
        Handle special commands like schema queries, database switches, etc.
        
        Returns:
            Tuple of (response, sql_query) if handled, None otherwise
        """
        query_lower = user_query.lower().strip()
        
        # Handle database switch commands
        if self.parser.is_database_switch_command(user_query):
            db_name = self.parser.extract_database_name(user_query)
            if db_name:
                return self._switch_database(db_name)
        
        # Handle list databases
        if self.parser.is_list_databases_command(user_query):
            return self._handle_list_databases()
        
        # Handle list tables (with optional database)
        if self.parser.is_list_tables_command(user_query):
            target_db = database or self.current_database
            db_name = self.parser.extract_database_name(user_query)
            if db_name:
                target_db = db_name
            return self._handle_list_tables(target_db)
        
        # Handle schema commands
        if self.parser.is_schema_command(user_query):
            target_db = database or self.current_database
            table_name = self.parser.extract_table_name_from_schema_command(user_query)
            if table_name:
                return self._handle_show_schema(table_name, target_db)
        
        return None
    
    def _switch_database(self, database: str) -> Tuple[str, Optional[str]]:
        """Switch to a different database"""
        success = db_connection.set_active_database(database)
        
        if success:
            self.current_database = database
            return f"Switched to database: {database}", None
        else:
            return f"Failed to switch to database '{database}'. Please check if it exists.", None
    
    def _handle_list_databases(self) -> Tuple[str, Optional[str]]:
        """Handle list databases command"""
        try:
            databases = self.introspector.list_databases()
            current = db_connection.get_active_database()
            
            # Format response
            db_list = "\n".join([f"- {db}" for db in databases])
            response = f"Available databases:\n{db_list}"
            
            if current:
                response += f"\n\nCurrent database: {current}"
            
            return response, None
        except Exception as e:
            return f"Failed to list databases: {str(e)}", None
    
    def _handle_list_tables(self, database: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """Handle list tables command"""
        try:
            tables = self.introspector.list_tables(database)
            active_db = database or self.current_database or "current"
            
            if not tables:
                return f"No tables found in database '{active_db}'.", None
            
            table_list = "\n".join([f"- {t}" for t in tables])
            return f"Tables in '{active_db}':\n{table_list}", None
        except Exception as e:
            return f"Failed to list tables: {str(e)}", None
    
    def _handle_show_schema(
        self, 
        table_name: str, 
        database: Optional[str] = None
    ) -> Tuple[str, Optional[str]]:
        """Handle show schema command"""
        try:
            schema = self.introspector.get_table_schema(table_name, database)
            active_db = database or self.current_database or "current"
            
            if not schema:
                # Table doesn't exist - suggest available tables
                try:
                    tables = self.introspector.list_tables(database)
                    table_list = "\n".join([f"- {t}" for t in tables[:10]])
                    return (
                        f"Table '{table_name}' does not exist in database '{active_db}'.\n\n"
                        f"Available tables:\n{table_list}",
                        None
                    )
                except:
                    return f"Table '{table_name}' does not exist in database '{active_db}'.", None
            
            # Format schema
            schema_lines = []
            for col in schema:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                default = f" DEFAULT {col['default']}" if col["default"] else ""
                line = f"- {col['name']}: {col['type']} {nullable}{default}"
                schema_lines.append(line)
            
            schema_str = "\n".join(schema_lines)
            return f"Schema for '{table_name}' in '{active_db}':\n{schema_str}", None
            
        except Exception as e:
            return f"Failed to get schema: {str(e)}", None
    
    def _generate_sql(
        self, 
        user_query: str, 
        database: Optional[str] = None
    ) -> Optional[str]:
        """Generate SQL with retry logic"""
        
        for attempt in range(self.max_generation_retries):
            try:
                # Build prompt
                prompt = self.prompt_builder.build_sql_generation_prompt(
                    user_query, 
                    database
                )
                
                # Get LLM response
                response = self.llm.generate(prompt)
                
                # Extract SQL
                sql = self.parser.extract_sql(response)
                
                if sql:
                    logger.info(f"Generated SQL on attempt {attempt + 1}: {sql[:100]}")
                    return sql
                
                logger.warning(f"Attempt {attempt + 1}: No SQL extracted from response")
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                continue
        
        return None
    
    def _execute_with_intelligent_retry(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[List[Dict], List[str], int, str]]:
        """
        Execute SQL with intelligent error handling and retry.
        
        Returns:
            Tuple of (results, columns, rows_affected, final_sql) or None
            If error: returns error message string as first element
        """
        # Determine target database
        target_db = database or self.current_database
        
        # Check if intelligent retry is enabled
        if self.enable_intelligent_retry:
            result = self.executor.execute_with_intelligent_retry(
                sql=sql,
                database=target_db,
                llm_client=self.llm,
                max_retries=self.max_execution_retries
            )
            
            results, columns, final_sql_or_error = result
            
            if results is None:
                # Execution failed - return error message
                return (final_sql_or_error, [], 0, sql)
            
            return (results, columns, len(results), final_sql_or_error)
        
        # Fall back to simple retry without intelligent repair
        return self._execute_with_simple_retry(sql, target_db)
    
    def _execute_with_simple_retry(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[List[Dict], List[str], int, str]]:
        """Execute SQL with simple retry logic (no intelligent repair)"""
        
        for attempt in range(self.max_execution_retries):
            try:
                query_type = self.safety.classify_query(sql)
                
                # For SELECT queries
                if query_type in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]:
                    results, columns = self.executor.execute_query(sql, database)
                    return results, columns, len(results), sql
                
                # For modification queries
                else:
                    rows_affected = self.executor.get_affected_rows(sql, database)
                    return [], [], rows_affected, sql
                    
            except Exception as e:
                logger.error(f"Execution attempt {attempt + 1} failed: {e}")
                if attempt == self.max_execution_retries - 1:
                    return (str(e), [], 0, sql)
                continue
        
        return None
    
    def _execute_with_retry(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[List[Dict], List[str], int]]:
        """Execute SQL with retry logic (legacy method for compatibility)"""
        
        for attempt in range(self.max_execution_retries):
            try:
                query_type = self.safety.classify_query(sql)
                
                # For SELECT queries
                if query_type in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]:
                    results, columns = self.executor.execute_query(sql, database)
                    return results, columns, len(results)
                
                # For modification queries
                else:
                    rows_affected = self.executor.get_affected_rows(sql, database)
                    return [], [], rows_affected
                    
            except Exception as e:
                logger.error(f"Execution attempt {attempt + 1} failed: {e}")
                if attempt == self.max_execution_retries - 1:
                    return None
                continue
        
        return None
    
    def list_databases(self) -> List[str]:
        """List all databases"""
        return self.introspector.list_databases()
    
    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """List tables in a database"""
        return self.introspector.list_tables(database)
    
    def get_table_schema(
        self, 
        table_name: str, 
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get table schema"""
        return self.introspector.get_table_schema(table_name, database)
    
    def get_current_database(self) -> Optional[str]:
        """Get the currently active database"""
        return self.current_database
    
    def switch_database(self, database: str) -> bool:
        """Switch to a different database"""
        success = db_connection.set_active_database(database)
        if success:
            self.current_database = database
        return success
    
    def resolve_ambiguity(
        self, 
        user_response: str, 
        possible_matches: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Resolve table/database ambiguity from user response"""
        resolution = self.parser.parse_ambiguity_resolution(user_response)
        
        if resolution == "first":
            return possible_matches[0]["database"]
        elif resolution == "second" and len(possible_matches) > 1:
            return possible_matches[1]["database"]
        elif resolution:
            return resolution
        
        return None


# Factory function
def create_agent(
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: Optional[str] = None,
    api_key: Optional[str] = None,
    force_client: Optional[str] = None
) -> EmendoAIAgent:
    """
    Create an EmendoAI agent instance.
    
    Client selection priority:
    1. force_client parameter (if specified)
    2. USE_CLAUDE_CLI env var
    3. USE_BEDROCK env var
    4. Default: Anthropic API
    """
    return EmendoAIAgent(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        api_key=api_key,
        force_client=force_client
    )
