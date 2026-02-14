"""
Response parser for EmendoAI agent
Parses LLM responses and extracts SQL queries
"""
import re
from typing import Optional, Tuple, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Stop words to ignore when extracting table names
SCHEMA_STOP_WORDS = {
    'for', 'of', 'the', 'a', 'an', 'in', 'to', 'from', 'with',
    'table', 'tables', 'schema', 'describe', 'show', 'me', 'all', 'get'
}


class ResponseParser:
    """Parses LLM responses to extract SQL and other information"""
    
    def extract_sql(self, response: str) -> Optional[str]:
        """Extract SQL query from LLM response"""
        # Try to find SQL in code blocks
        code_block_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if code_block_match:
            return code_block_match.group(1).strip()
        
        # Try to find SQL in regular code blocks
        code_match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
        if code_match:
            potential_sql = code_match.group(1).strip()
            if self._looks_like_sql(potential_sql):
                return potential_sql
        
        # Try to find standalone SQL
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']
        
        for keyword in sql_keywords:
            pattern = rf'\b{keyword}\b'
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                sql = response[match.start():].strip()
                sql = self._cleanup_sql(sql)
                if sql:
                    return sql
        
        cleaned = response.strip()
        if self._looks_like_sql(cleaned):
            return cleaned
        
        return None
    
    def extract_table_name_from_schema_command(self, user_input: str) -> Optional[str]:
        """Extract table name from schema-related commands."""
        normalized = user_input.lower().strip()
        
        # More comprehensive patterns
        schema_patterns = [
            r'^show\s+schema\s+(?:for\s+|of\s+)?(.+)',
            r'^show\s+(\w+)\s+schema',  # "show users schema"
            r'^describe\s+(?:table\s+)?(.+)',
            r'^desc\s+(?:table\s+)?(.+)',
            r'^(\w+)\s+schema',  # "users schema"
        ]
        
        for pattern in schema_patterns:
            match = re.match(pattern, normalized)
            if match:
                captured = match.group(1).strip()
                table_name = self._extract_last_token(captured)
                if table_name:
                    logger.debug(f"Extracted table name '{table_name}' from schema command")
                    return table_name
        
        # Fallback
        words = normalized.split()
        meaningful_words = [w for w in words if w not in SCHEMA_STOP_WORDS]
        
        if meaningful_words:
            return meaningful_words[-1]
        
        return None
    
    def _extract_last_token(self, text: str) -> Optional[str]:
        """Extract the last meaningful token from text."""
        tokens = text.split()
        
        # Filter out stop words
        meaningful_tokens = [t for t in tokens if t.lower() not in SCHEMA_STOP_WORDS]
        
        if not meaningful_tokens:
            if tokens:
                return tokens[-1]
            return None
        
        return meaningful_tokens[-1]
    
    def extract_database_name(self, user_input: str) -> Optional[str]:
        """Extract database name from user input."""
        normalized = user_input.lower().strip()
        
        # Handle multi-word database names like "timechain finance"
        # Pattern: "in timechain finance" or "list tables in timechain finance"
        match = re.search(r'(?:in|list\s+tables?\s+in|show\s+tables?\s+in)\s+(.+)', normalized)
        if match:
            captured = match.group(1).strip()
            # Remove trailing words like "tables"
            captured = re.sub(r'\s+tables?\s*$', '', captured)
            return captured.strip()
        
        # Handle single word database names
        db_patterns = [
            r'use\s+(\w+)',
            r'switch\s+to\s+(\w+)',
            r'connect\s+to\s+(\w+)',
        ]
        
        for pattern in db_patterns:
            match = re.search(pattern, normalized)
            if match:
                return match.group(1)
        
        return None
    
    def is_schema_command(self, user_input: str) -> bool:
        """Check if input is a schema-related command."""
        normalized = user_input.lower().strip()
        
        schema_keywords = [
            'show schema', 'describe', 'desc ',
            'show columns', 'list columns'
        ]
        
        # Also check for "X schema" pattern
        if re.match(r'^\w+\s+schema', normalized):
            return True
        
        return any(keyword in normalized for keyword in schema_keywords)
    
    def is_database_switch_command(self, user_input: str) -> bool:
        """Check if input is a database switch command."""
        normalized = user_input.lower().strip()
        
        switch_keywords = [
            'use ', 'switch to', 'connect to',
            'change database', 'change db'
        ]
        
        return any(keyword in normalized for keyword in switch_keywords)
    
    def is_list_databases_command(self, user_input: str) -> bool:
        """Check if user wants to list databases."""
        normalized = user_input.lower().strip()
        
        # More flexible patterns
        list_db_patterns = [
            r'^list\s+databases?',
            r'^show\s+databases?',
            r'^get\s+databases?',
            r'^all\s+databases',
            r'^list\s+all\s+databases',
        ]
        
        return any(re.match(pattern, normalized) for pattern in list_db_patterns)
    
    def is_list_tables_command(self, user_input: str) -> bool:
        """Check if user wants to list tables."""
        normalized = user_input.lower().strip()
        
        list_table_patterns = [
            r'^list\s+tables?',
            r'^show\s+tables?',
            r'^get\s+tables?',
        ]
        
        # Also match "list tables in <database>" patterns
        if re.search(r'(?:list|show|get)\s+tables?\s+(?:in|from|of)\s+', normalized):
            return True
        
        return any(re.match(pattern, normalized) for pattern in list_table_patterns)
    
    def _looks_like_sql(self, text: str) -> bool:
        """Check if text looks like a SQL query"""
        sql_starters = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 
            'CREATE', 'DROP', 'ALTER', 'TRUNCATE',
            'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN'
        ]
        
        text_upper = text.strip().upper()
        return any(text_upper.startswith(kw) for kw in sql_starters)
    
    def _cleanup_sql(self, sql: str) -> str:
        """Clean up extracted SQL"""
        sql = sql.strip()
        
        lines = sql.split('\n')
        clean_lines = []
        
        for line in lines:
            if re.match(r'^(the|this|here|below|above|note|note:|warning|error)', line.lower()):
                continue
            clean_lines.append(line)
        
        sql = '\n'.join(clean_lines).strip()
        
        while sql.endswith(';') or sql.endswith('`'):
            sql = sql[:-1].strip()
        
        return sql
    
    def parse_ambiguity_resolution(self, response: str) -> Optional[str]:
        """Parse user's response to resolve ambiguity"""
        response_lower = response.lower().strip()
        
        match = re.search(r'use\s+(\w+)', response_lower)
        if match:
            return match.group(1)
        
        match = re.search(r'database\s+(\w+)', response_lower)
        if match:
            return match.group(1)
        
        if 'first' in response_lower:
            return "first"
        
        if 'second' in response_lower:
            return "second"
        
        return None
    
    def parse_permission_response(self, response: str) -> bool:
        """Parse user's response to permission request"""
        positive_responses = ['yes', 'y', 'yeah', 'sure', 'go ahead', 'proceed', 'do it', 'execute']
        negative_responses = ['no', 'n', 'nope', 'cancel', 'stop', 'abort']
        
        response_lower = response.lower().strip()
        
        for positive in positive_responses:
            if positive in response_lower:
                return True
        
        for negative in negative_responses:
            if negative in response_lower:
                return False
        
        return False
    
    def format_results_summary(
        self, 
        results: List[Dict[str, Any]], 
        columns: List[str],
        query: str
    ) -> str:
        """Format query results into natural language summary"""
        if not results:
            return "No results found for your query."
        
        if not columns:
            return "Query executed successfully but returned no columns."
        
        # For single row results
        if len(results) == 1:
            row = results[0]
            if len(columns) == 1:
                return f"The result is: {row.get(columns[0], 'N/A')}"
            else:
                items = [f"{col}: {row.get(col, 'N/A')}" for col in columns if row.get(col) is not None]
                return "The result is: " + ", ".join(items)
        
        # For multiple rows - show ALL results without truncation
        summary = f"Found {len(results)} results:\n\n"
        
        # Show all rows, not just first few
        for i, row in enumerate(results, 1):
            items = [f"{col}: {row.get(col, 'N/A')}" for col in columns[:5]]  # Limit to first 5 cols
            summary += f"{i}. {', '.join(items)}\n"
            
            if len(columns) > 5:
                summary += f"   (+ {len(columns) - 5} more columns)\n"
        
        return summary
    
    def format_execution_result(
        self,
        query_type: str,
        rows_affected: int,
        results: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """Format execution result into natural language"""
        if query_type == "SELECT":
            if results is None or len(results) == 0:
                return "Query executed successfully. No rows returned."
            
            # Safely get columns
            columns = []
            if results and len(results) > 0:
                first_result = results[0]
                if isinstance(first_result, dict):
                    columns = list(first_result.keys())
                elif isinstance(first_result, (list, tuple)):
                    columns = [f"col_{i}" for i in range(len(first_result))]
            
            return self.format_results_summary(results, columns, query_type)
        
        elif query_type == "INSERT":
            return f"Successfully inserted {rows_affected} row(s)."
        
        elif query_type == "UPDATE":
            return f"Successfully updated {rows_affected} row(s)."
        
        elif query_type == "DELETE":
            return f"Successfully deleted {rows_affected} row(s)."
        
        elif query_type == "CREATE":
            return f"Successfully created the table (or other object)."
        
        elif query_type == "DROP":
            return f"Successfully dropped the table (or other object)."
        
        else:
            return f"Query executed successfully. {rows_affected} row(s) affected."


# Singleton instance
response_parser = ResponseParser()
