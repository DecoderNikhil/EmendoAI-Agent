"""
Main EmendoAI Agent
Ties all components together and handles the full flow
"""
from typing import Tuple, Optional, List, Dict, Any
import logging

from config import settings
from src.llm.anthropic_client import ClaudeClient
from src.database.introspection import schema_introspector
from src.database.executor import query_executor
from src.sql.validator import sql_validator
from src.agent.prompt_builder import prompt_builder
from src.agent.safety import safety_checker
from src.agent.response_parser import response_parser

logger = logging.getLogger(__name__)


class EmendoAIAgent:
    """
    Main AI Agent that converts natural language to SQL and back
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.llm = ClaudeClient(api_key)
        self.introspector = schema_introspector
        self.executor = query_executor
        self.validator = sql_validator
        self.prompt_builder = prompt_builder
        self.safety = safety_checker
        self.parser = response_parser
        
        # Retry settings
        self.max_generation_retries = settings.MAX_GENERATION_RETRIES
        self.max_execution_retries = settings.MAX_EXECUTION_RETRIES
        
        # State
        self.current_database: Optional[str] = None
        self.conversation_history: List[Dict[str, str]] = []
    
    def process_query(
        self, 
        user_query: str,
        database: Optional[str] = None,
        user_approved: bool = False
    ) -> Tuple[str, Optional[str]]:
        """
        Process a natural language query
        
        Returns:
            Tuple of (response, sql_query)
            - response: Natural language response to user
            - sql_query: The SQL that was executed (None if not executed)
        """
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
        
        # Step 5: Execute the query
        result = self._execute_with_retry(sql, target_db)
        
        if result is None:
            return "Query execution failed. Please check the SQL and try again.", sql
        
        results, columns, rows_affected = result
        
        # Step 6: Format response
        query_type = self.safety.classify_query(sql)
        
        # Add warning if applicable
        warning = f"{warning_msg}\n\n" if needs_warning else ""
        
        formatted_result = self.parser.format_execution_result(
            query_type, 
            rows_affected, 
            results if query_type == "SELECT" else None
        )
        
        # Update current database if changed
        if query_type == "CREATE" or "DATABASE" in sql.upper():
            # Refresh database list
            pass
        
        response = f"{warning}{formatted_result}"
        
        return response, sql
    
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
    
    def _execute_with_retry(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[List[Dict], List[str], int]]:
        """Execute SQL with retry logic"""
        
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
def create_agent(api_key: Optional[str] = None) -> EmendoAIAgent:
    """Create an EmendoAI agent instance"""
    return EmendoAIAgent(api_key)
