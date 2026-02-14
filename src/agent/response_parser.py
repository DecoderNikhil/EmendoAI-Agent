"""
Response parser for EmendoAI agent
Parses LLM responses and extracts SQL queries
"""
import re
from typing import Optional, Tuple, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# PART 3: Stop words to remove from table name extraction
TABLE_EXTRACT_STOP_WORDS = {
    'show', 'schema', 'for', 'of', 'the', 'a', 'an', 'in', 'to', 'from', 'with',
    'table', 'tables', 'describe', 'me', 'all', 'get'
}


class ResponseParser:
    """Parses LLM responses to extract SQL and other information"""
    
    def extract_sql(self, response: str) -> Optional[str]:
        """Extract SQL query from LLM response"""
        # Try code blocks
        code_block_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if code_block_match:
            return code_block_match.group(1).strip()
        
        # Try regular code blocks
        code_match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
        if code_match:
            potential_sql = code_match.group(1).strip()
            if self._looks_like_sql(potential_sql):
                return potential_sql
        
        # Try standalone SQL
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
        """
        PART 3: Extract table name from schema commands.
        Remove stop words and extract last meaningful token.
        
        Examples:
        "show schema for users" -> "users"
        "show users schema" -> "users"
        "describe users" -> "users"
        """
        normalized = user_input.lower().strip()
        
        # First, extract the table name directly from the input
        # Strategy: find position after key prepositions and get the last word
        
        # Handle "show schema for X" -> get X
        match = re.search(r'show\s+schema\s+for\s+(\S+)', normalized)
        if match:
            return match.group(1)
        
        # Handle "describe X" or "desc X" -> get X
        match = re.search(r'(?:describe|desc)\s+(\S+)', normalized)
        if match:
            return match.group(1)
        
        # Handle "show X schema" -> get X
        match = re.search(r'show\s+(\S+)\s+schema', normalized)
        if match:
            return match.group(1)
        
        # Handle "show columns from X" -> get X
        match = re.search(r'show\s+columns?\s+from\s+(\S+)', normalized)
        if match:
            return match.group(1)
        
        # Handle "X schema" where X is at the end
        match = re.search(r'(\S+)\s+schema$', normalized)
        if match:
            return match.group(1)
        
        # Fallback: remove common patterns and get last meaningful word
        patterns_to_remove = [
            r'^show\s+schema\s+for\s+',
            r'^describe\s+',
            r'^desc\s+',
            r'^show\s+columns?\s+from\s+',
            r'^show\s+',
            r'^get\s+schema\s+for\s+',
            r'^list\s+columns\s+',
            r'\s+schema$',
            r'\s+table$',
        ]
        
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
        
        # Also remove leading "for " if present
        if normalized.startswith('for '):
            normalized = normalized[4:]
        
        # Strip whitespace
        normalized = normalized.strip()
        
        # Split into words and filter stop words
        words = normalized.split()
        
        meaningful_words = [w for w in words if w.lower() not in TABLE_EXTRACT_STOP_WORDS]
        
        if meaningful_words:
            return meaningful_words[-1]
        
        # Last resort: return last word
        if words:
            return words[-1]
        
        return None
    
    def extract_database_name(self, user_input: str) -> Optional[str]:
        """Extract database name from user input with normalization support."""
        normalized = user_input.lower().strip()
        
        # Handle "list tables in <database>" pattern - handle "list all the tables in dvdrental"
        match = re.search(r'(?:list|show|get)\s+(?:all\s+)?(?:the\s+)?tables?\s+(?:in|from|of)\s+(.+)', normalized)
        if match:
            captured = match.group(1).strip()
            # Remove trailing words
            captured = re.sub(r'\s+(?:tables?)\s*$', '', captured)
            return captured.strip() if captured else None
        
        # Handle "use <database>" pattern
        match = re.search(r'use\s+(\S+)', normalized)
        if match:
            return match.group(1)
        
        # Handle "switch to <database>" pattern
        match = re.search(r'switch\s+to\s+(\S+)', normalized)
        if match:
            return match.group(1)
        
        return None
    
    def is_schema_command(self, user_input: str) -> bool:
        """Check if input is a schema command."""
        normalized = user_input.lower().strip()
        
        # More comprehensive schema command detection
        # Pattern: show <table> schema, show schema for <table>, describe <table>, etc.
        schema_patterns = [
            r'^show\s+schema',  # show schema, show schema for X
            r'^describe\s+',     # describe X
            r'^desc\s+',         # desc X  
            r'^show\s+columns',  # show columns from X
            r'^list\s+columns',   # list columns from X
            r'\s+schema$',       # X schema (at end)
        ]
        
        for pattern in schema_patterns:
            if re.search(pattern, normalized):
                return True
        
        return False
    
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
        
        # Match patterns with "in <database>" - handle "list all the tables in dvdrental"
        # Match list/list all/show/get followed by tables and then in/from/of
        if re.search(r'(?:list|show|get)\s+(?:all\s+)?(?:the\s+)?tables?\s+(?:in|from|of)\s+', normalized):
            return True
        
        # Also handle just "list tables in dvdrental" without "all the"
        if re.search(r'(?:list|show|get)\s+tables?\s+(?:in|from|of)\s+', normalized):
            return True
        
        list_table_patterns = [
            r'^list\s+tables?$',
            r'^show\s+tables?$',
            r'^get\s+tables?$',
            r'^list\s+all\s+tables?$',
        ]
        
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
        
        match = re.search(r'use\s+(\S+)', response_lower)
        if match:
            return match.group(1)
        
        match = re.search(r'database\s+(\S+)', response_lower)
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
        """Format query results - show ALL results without truncation"""
        if not results:
            return "No results found for your query."
        
        if not columns:
            return "Query executed successfully but returned no columns."
        
        if len(results) == 1:
            row = results[0]
            if len(columns) == 1:
                return f"The result is: {row.get(columns[0], 'N/A')}"
            else:
                items = [f"{col}: {row.get(col, 'N/A')}" for col in columns if row.get(col) is not None]
                return "The result is: " + ", ".join(items)
        
        # Show ALL results
        summary = f"Found {len(results)} results:\n\n"
        
        for i, row in enumerate(results, 1):
            items = [f"{col}: {row.get(col, 'N/A')}" for col in columns[:5]]
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
