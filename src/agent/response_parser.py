"""
Response parser for EmendoAI agent
Parses LLM responses and extracts SQL queries
"""
import re
from typing import Optional, Tuple, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ResponseParser:
    """Parses LLM responses to extract SQL and other information"""
    
    def extract_sql(self, response: str) -> Optional[str]:
        """
        Extract SQL query from LLM response
        
        The LLM should return just the SQL, but sometimes it might
        include explanations or markdown code blocks
        """
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
        
        # Try to find standalone SQL (starts with SELECT, INSERT, etc.)
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']
        
        for keyword in sql_keywords:
            pattern = rf'\b{keyword}\b'
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                # Extract from the keyword to the end
                sql = response[match.start():].strip()
                # Clean up any trailing text
                sql = self._cleanup_sql(sql)
                if sql:
                    return sql
        
        # If nothing else worked, return the whole response trimmed
        cleaned = response.strip()
        if self._looks_like_sql(cleaned):
            return cleaned
        
        return None
    
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
        # Remove trailing semicolons and extra whitespace
        sql = sql.strip()
        
        # Remove any trailing non-SQL text
        # Find the last semicolon or SQL keyword ending
        lines = sql.split('\n')
        clean_lines = []
        
        for line in lines:
            # Skip lines that look like explanations
            if re.match(r'^(the|this|here|below|above|note|note:|warning|error)', line.lower()):
                continue
            clean_lines.append(line)
        
        sql = '\n'.join(clean_lines).strip()
        
        # Remove trailing semicolons and whitespace
        while sql.endswith(';') or sql.endswith('`'):
            sql = sql[:-1].strip()
        
        return sql
    
    def parse_ambiguity_resolution(self, response: str) -> Optional[str]:
        """Parse user's response to resolve ambiguity"""
        # User might say "use database X" or "the first one" or "dbname.tablename"
        
        response_lower = response.lower().strip()
        
        # Try to extract database name
        # Pattern: "use <database>"
        match = re.search(r'use\s+(\w+)', response_lower)
        if match:
            return match.group(1)
        
        # Pattern: "database <name>"
        match = re.search(r'database\s+(\w+)', response_lower)
        if match:
            return match.group(1)
        
        # Pattern: "first" or "the first"
        if 'first' in response_lower:
            return "first"
        
        # Pattern: "second", "third", etc.
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
        
        # Default to False for safety
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
                # Single column, single row
                return f"The result is: {row[columns[0]]}"
            else:
                # Multiple columns, single row
                items = [f"{col}: {row[col]}" for col in columns if row.get(col) is not None]
                return "The result is: " + ", ".join(items)
        
        # For multiple rows
        if len(results) <= 5:
            # List out each row
            summary = f"Found {len(results)} results:\n\n"
            for i, row in enumerate(results, 1):
                items = [f"{col}: {row.get(col, 'N/A')}" for col in columns[:3]]  # Limit to first 3 cols
                summary += f"{i}. {', '.join(items)}\n"
            
            if len(columns) > 3:
                summary += f"\n(Note: {len(columns) - 3} more columns not shown)"
            
            return summary
        
        # For many rows - provide summary
        summary = f"Found {len(results)} rows. Here's a summary:\n\n"
        
        # Aggregate numeric columns if any
        for col in columns:
            if results and col in results[0]:
                sample_value = results[0].get(col)
                if isinstance(sample_value, (int, float)):
                    values = [r.get(col, 0) for r in results if r.get(col) is not None]
                    total = sum(values)
                    avg = total / len(values) if values else 0
                    summary += f"- {col}: total={total}, average={avg:.2f}\n"
        
        # Show first few rows
        summary += "\nFirst few rows:\n"
        for row in results[:3]:
            items = [str(row.get(col, '')) for col in columns[:3]]
            summary += "- " + " | ".join(items) + "\n"
        
        if len(results) > 3:
            summary += f"\n... and {len(results) - 3} more rows"
        
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
            return self.format_results_summary(results, list(results[0].keys()) if results else [], query_type)
        
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
