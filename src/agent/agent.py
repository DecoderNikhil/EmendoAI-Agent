"""
Main EmendoAI Agent
Ties all components together and handles the full flow
"""
import re
from typing import Tuple, Optional, List, Dict, Any
import logging

from config import settings
from src.llm.anthropic_client import ClaudeClient
from src.llm.bedrock_client import BedrockClient
from src.llm.cli_client import CLIClaudeClient
from src.database.introspection import schema_introspector
from src.database.executor import query_executor, SQLValidator, SYSTEM_TABLES
from src.database.connection import db_connection
from src.sql.validator import sql_validator
from src.agent.prompt_builder import prompt_builder
from src.agent.safety import safety_checker
from src.agent.response_parser import response_parser

logger = logging.getLogger(__name__)


def _create_llm_client():
    if settings.USE_CLAUDE_CLI:
        return CLIClaudeClient()
    if settings.USE_BEDROCK:
        return BedrockClient(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
    return ClaudeClient(settings.ANTHROPIC_API_KEY)


class EmendoAIAgent:
    """Main AI Agent that converts natural language to SQL and back."""
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        api_key: Optional[str] = None,
        force_client: Optional[str] = None
    ):
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
            self.llm = _create_llm_client()
        
        self.introspector = schema_introspector
        self.executor = query_executor
        self.validator = sql_validator
        self.prompt_builder = prompt_builder
        self.safety = safety_checker
        self.parser = response_parser
        
        self.max_generation_retries = settings.MAX_GENERATION_RETRIES
        self.max_execution_retries = settings.MAX_EXECUTION_RETRIES
        
        self.current_database: Optional[str] = db_connection.get_active_database()
        self.conversation_history: List[Dict[str, str]] = []
        self.enable_intelligent_retry = settings.ENABLE_INTELLIGENT_RETRY
    
    def _extract_database_from_query(self, user_query: str) -> Optional[str]:
        """Extract database name from query like 'count films in dvdrental'"""
        normalized = user_query.lower()
        
        # Patterns to extract database name
        patterns = [
            r'in\s+(\S+)',                    # in dvdrental
            r'from\s+(\S+)_database',        # from dvdrental_database
            r'using\s+(\S+)',                 # using dvdrental
            r'database\s+(\S+)',              # database dvdrental
        ]
        
        for pattern in patterns:
            match = re.search(pattern, normalized)
            if match:
                db_name = match.group(1).strip()
                # Clean up common trailing words
                db_name = re.sub(r'\s+(table|tables|query)$', '', db_name)
                return db_name
        
        return None
    
    def process_query(
        self, 
        user_query: str,
        database: Optional[str] = None,
        user_approved: bool = False
    ) -> Tuple[str, Optional[str]]:
        """Process a natural language query."""
        
        # Handle special commands first
        special_response = self._handle_special_command(user_query, database)
        if special_response:
            return special_response
        
        # Check if database is mentioned in query
        db_from_query = self._extract_database_from_query(user_query)
        
        # If database specified in query, find and switch to it
        if db_from_query and not database:
            found_db = self.introspector.find_database(db_from_query)
            if found_db:
                db_connection.set_active_database(found_db)
                self.current_database = found_db
                logger.info(f"Switched to database from query: {found_db}")
        
        # Determine target database
        target_db = database or self.current_database
        
        # Generate SQL from natural language
        sql = self._generate_sql(user_query, target_db)
        
        if not sql:
            return "I couldn't generate a SQL query from your request. Could you rephrase it?", None
        
        # Validate SQL
        is_valid, error_msg = self.validator.validate(sql)
        if not is_valid:
            return f"Generated SQL is invalid: {error_msg}", sql
        
        # Check safety
        requires_permission, _ = self.safety.requires_permission(sql)
        if requires_permission and not user_approved:
            permission_msg = self.safety.format_permission_message(sql)
            return f"{permission_msg}\n\nGenerated SQL:\n```sql\n{sql}\n```", sql
        
        # PART 4: Verify tables exist before execution
        # Loop to handle cross-database resolution
        max_verification_attempts = 2
        for attempt in range(max_verification_attempts):
            valid, error = self.executor.verify_tables_exist(sql, target_db)
            
            if valid:
                break  # Tables exist, proceed
                
            # Table not found - try cross-database resolution
            match = re.search(r"Tables not found in database '([^']+)':\s*(.+)", error)
            if not match:
                break  # Different error, can't resolve
            
            table_name = match.group(2).strip()
            matches = self.introspector.find_table_across_all_databases(table_name)
            
            if len(matches) == 1:
                # Found in one database - switch and regenerate
                found = matches[0]
                db_connection.set_active_database(found['database'])
                self.current_database = found['database']
                logger.info(f"Switched to database: {found['database']} (found table {table_name})")
                
                # Regenerate SQL with new database context
                sql = self._generate_sql(user_query, found['database'])
                if sql:
                    target_db = found['database']
                    logger.info(f"Regenerated SQL for {found['database']}: {sql[:50]}")
                    # Loop continues to verify again
                else:
                    break  # Could not regenerate
            elif len(matches) > 1:
                options = "\n".join([f"- {m['database']}" for m in matches])
                return (
                    f"Table '{table_name}' exists in multiple databases:\n{options}\n\n"
                    f"Please specify the database in your query (e.g., 'count {table_name} in <database>')",
                    sql
                )
            else:
                break  # Table not found anywhere
        
        # After verification loop, check if we have a valid query
        if target_db:
            valid, error = self.executor.verify_tables_exist(sql, target_db)
            if not valid:
                return self._handle_table_not_found_in_sql(error, user_query), sql
        
        # Check for UPDATE warnings
        needs_warning, warning_msg = self.safety.needs_warning(sql)
        
        # Execute the query
        result = self._execute_with_intelligent_retry(sql, target_db, user_query=user_query)
        
        if result is None:
            return "Query execution failed.", sql
        
        if isinstance(result, str):
            return result, sql
        
        results, columns, rows_affected, final_sql = result
        
        query_type = self.safety.classify_query(final_sql)
        warning = f"{warning_msg}\n\n" if needs_warning else ""
        
        repair_info = ""
        if final_sql != sql and self.enable_intelligent_retry:
            repair_info = f"_Note: SQL was automatically corrected_\n\n"
        
        formatted_result = self.parser.format_execution_result(
            query_type, 
            rows_affected, 
            results if query_type == "SELECT" else None
        )
        
        response = f"{warning}{repair_info}{formatted_result}"
        return response, final_sql
    
    def _handle_special_command(
        self, 
        user_query: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Handle special commands."""
        
        # Database switch
        if self.parser.is_database_switch_command(user_query):
            db_name = self.parser.extract_database_name(user_query)
            if db_name:
                return self._switch_database(db_name)
        
        # List databases
        if self.parser.is_list_databases_command(user_query):
            return self._handle_list_databases()
        
        # List tables
        if self.parser.is_list_tables_command(user_query):
            db_name = self.parser.extract_database_name(user_query)
            return self._handle_list_tables(db_name)
        
        # Schema command - search across ALL databases
        if self.parser.is_schema_command(user_query):
            table_name = self.parser.extract_table_name_from_schema_command(user_query)
            if table_name:
                return self._handle_show_schema_advanced(table_name)
        
        return None
    
    def _handle_list_databases(self) -> Tuple[str, Optional[str]]:
        """PART 1: List databases - exclude system databases."""
        try:
            databases = self.introspector.list_databases(include_system=False)
            current = db_connection.get_active_database()
            
            if not databases:
                return "No user databases found.", None
            
            db_list = "\n".join([f"- {db}" for db in databases])
            response = f"Available databases:\n{db_list}"
            
            if current:
                response += f"\n\nCurrent database: {current}"
            
            return response, None
        except Exception as e:
            return f"Failed to list databases: {str(e)}", None
    
    def _handle_list_tables(self, database: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """List tables in a database with fuzzy matching."""
        target_db = database or self.current_database
        
        if not target_db:
            return "Please specify a database. Use 'list tables in <database>' or switch to a database first.", None
        
        # Find database with fuzzy matching
        found_db = self.introspector.find_database(target_db)
        if not found_db:
            databases = self.introspector.list_databases(include_system=False)
            db_list = "\n".join([f"- {db}" for db in databases])
            return f"Database '{target_db}' not found.\n\nAvailable databases:\n{db_list}", None
        
        # Switch to the database
        db_connection.set_active_database(found_db)
        self.current_database = found_db
        
        try:
            tables = self.introspector.list_tables(found_db)
            
            if not tables:
                return f"No tables found in database '{found_db}'.", None
            
            table_list = "\n".join([f"- {t}" for t in tables])
            return f"Tables in '{found_db}':\n{table_list}", None
        except Exception as e:
            return f"Failed to list tables: {str(e)}", None
    
    def _handle_show_schema_advanced(self, table_name: str) -> Tuple[str, Optional[str]]:
        """
        PART 2 & 3: Search across ALL databases for table.
        Handle ambiguity if table exists in multiple databases.
        """
        try:
            # Search across all databases
            matches = self.introspector.find_table_across_all_databases(table_name)
            
            if not matches:
                return self._handle_table_not_found(table_name), None
            
            if len(matches) == 1:
                # Found in one database - get schema
                match = matches[0]
                return self._get_schema_for_table(match['database'], match['table'])
            
            # Multiple matches - ask user
            options = []
            for i, match in enumerate(matches, 1):
                options.append(f"{i}. Database: {match['database']}, Table: {match['table']}")
            
            options_text = "\n".join(options)
            return (
                f"Table '{table_name}' found in multiple databases. Which one do you mean?\n\n"
                f"{options_text}\n\nPlease specify with 'show schema for <table> in <database>'",
                None
            )
            
        except Exception as e:
            return f"Failed to get schema: {str(e)}", None
    
    def _handle_table_not_found(self, table_name: str) -> str:
        """Show available tables from all databases when table not found."""
        all_tables_by_db = {}
        
        try:
            databases = self.introspector.list_databases(include_system=False)
            for db in databases:
                try:
                    tables = self.introspector.list_tables(db)
                    if tables:
                        all_tables_by_db[db] = tables
                except:
                    continue
        except:
            pass
        
        if not all_tables_by_db:
            return f"Table '{table_name}' not found in any database."
        
        response = f"Table '{table_name}' not found.\n\nTables by database:\n"
        for db, tables in all_tables_by_db.items():
            response += f"\n{db}:\n"
            response += "\n".join([f"  - {t}" for t in tables[:10]])
            if len(tables) > 10:
                response += f"\n  ... and {len(tables) - 10} more"
        
        return response
    
    def _handle_table_not_found_in_sql(self, error_msg: str, user_query: str) -> str:
        """Handle table not found during SQL execution."""
        # Try to find table in other databases
        import re
        match = re.search(r"Table '([^']+)'", error_msg)
        if match:
            table_name = match.group(1)
            matches = self.introspector.find_table_across_all_databases(table_name)
            
            if len(matches) == 1:
                # Found in one database - switch and suggest retry
                match = matches[0]
                return (
                    f"Table '{table_name}' not found in current database.\n"
                    f"It exists in database '{match['database']}'.\n"
                    f"Switch to that database and retry your query."
                )
            elif len(matches) > 1:
                options = "\n".join([f"- {m['database']}" for m in matches])
                return (
                    f"Table '{table_name}' not found in current database.\n"
                    f"It exists in multiple databases:\n{options}\n"
                    f"Please specify which database to use."
                )
        
        return error_msg
    
    def _get_schema_for_table(self, database: str, table_name: str) -> Tuple[str, Optional[str]]:
        """Get schema for a specific table."""
        try:
            # Switch to database
            db_connection.set_active_database(database)
            self.current_database = database
            
            schema = self.introspector.get_table_schema(table_name, database)
            
            if not schema:
                return f"Table '{table_name}' exists but has no columns in database '{database}'.", None
            
            schema_lines = []
            for col in schema:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                default = f" DEFAULT {col['default']}" if col['default'] else ""
                line = f"- {col['name']}: {col['type']} {nullable}{default}"
                schema_lines.append(line)
            
            schema_str = "\n".join(schema_lines)
            return f"Schema for '{table_name}' in '{database}':\n{schema_str}", None
            
        except Exception as e:
            return f"Failed to get schema: {str(e)}", None
    
    def _switch_database(self, database: str) -> Tuple[str, Optional[str]]:
        """Switch to a different database with fuzzy matching."""
        found_db = self.introspector.find_database(database)
        
        if not found_db:
            databases = self.introspector.list_databases(include_system=False)
            db_list = "\n".join([f"- {db}" for db in databases])
            return f"Database '{database}' not found.\n\nAvailable databases:\n{db_list}", None
        
        success = db_connection.set_active_database(found_db)
        
        if success:
            self.current_database = found_db
            return f"Switched to database: {found_db}", None
        else:
            return f"Failed to switch to database '{found_db}'.", None
    
    def _generate_sql(self, user_query: str, database: Optional[str] = None) -> Optional[str]:
        """Generate SQL with retry logic."""
        
        for attempt in range(self.max_generation_retries):
            try:
                prompt = self.prompt_builder.build_sql_generation_prompt(user_query, database)
                response = self.llm.generate(prompt)
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
        database: Optional[str] = None,
        user_query: str = None
    ) -> Optional[Tuple[List[Dict], List[str], int, str]]:
        """Execute SQL with intelligent handling."""
        target_db = database or self.current_database
        
        # PART 6: Log active database
        logger.info(f"Executing on database: {target_db}")
        
        if self.enable_intelligent_retry:
            result = self.executor.execute_with_intelligent_retry(
                sql=sql,
                database=target_db,
                llm_client=self.llm,
                user_query=user_query,
                max_retries=self.max_execution_retries
            )
            
            results, columns, final_sql_or_error = result
            
            if results is None:
                return final_sql_or_error
            
            return (results, columns, len(results), final_sql_or_error)
        
        return self._execute_with_simple_retry(sql, target_db)
    
    def _execute_with_simple_retry(
        self, 
        sql: str, 
        database: Optional[str] = None
    ) -> Optional[Tuple[List[Dict], List[str], int, str]]:
        """Execute SQL with simple retry."""
        
        for attempt in range(self.max_execution_retries):
            try:
                query_type = self.safety.classify_query(sql)
                
                if query_type in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]:
                    results, columns = self.executor.execute_query(sql, database)
                    return results, columns, len(results), sql
                else:
                    rows_affected = self.executor.get_affected_rows(sql, database)
                    return [], [], rows_affected, sql
                    
            except Exception as e:
                logger.error(f"Execution attempt {attempt + 1} failed: {e}")
                if attempt == self.max_execution_retries - 1:
                    return str(e)
                continue
        
        return None
    
    def list_databases(self) -> List[str]:
        return self.introspector.list_databases()
    
    def list_tables(self, database: Optional[str] = None) -> List[str]:
        return self.introspector.list_tables(database)
    
    def get_table_schema(self, table_name: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.introspector.get_table_schema(table_name, database)
    
    def get_current_database(self) -> Optional[str]:
        return self.current_database
    
    def switch_database(self, database: str) -> bool:
        success = db_connection.set_active_database(database)
        if success:
            self.current_database = database
        return success


def create_agent(**kwargs) -> EmendoAIAgent:
    return EmendoAIAgent(**kwargs)
