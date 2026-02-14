"""
Prompt builder for EmendoAI agent
Constructs prompts for Claude to generate SQL queries
"""
from typing import Dict, Any, List, Optional
import json

from src.database.introspection import schema_introspector


class PromptBuilder:
    """Builds prompts for the LLM to generate SQL"""
    
    def __init__(self):
        self.introspector = schema_introspector
    
    def build_sql_generation_prompt(
        self,
        user_query: str,
        database: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build a prompt for generating SQL from natural language
        """
        # Get database context
        db_context = self._get_database_context(database)
        
        # Build the prompt
        prompt = f"""You are a PostgreSQL expert. Convert the following natural language query to a valid PostgreSQL SQL query.

User Query: {user_query}

{db_context}

Instructions:
1. Generate a valid PostgreSQL SQL query
2. Only generate the SQL query, nothing else
3. Do not include explanations
4. Use proper PostgreSQL syntax
5. If the query is ambiguous, make a reasonable assumption based on the schema

SQL Query:"""
        
        return prompt
    
    def _get_database_context(self, database: Optional[str] = None) -> str:
        """Get database schema context for the prompt"""
        try:
            if database:
                tables = self.introspector.list_tables(database)
                context = f"\nDatabase: {database}\n"
                context += "Tables:\n"
                
                for table in tables:
                    try:
                        schema = self.introspector.get_table_schema(table, database)
                        columns = ", ".join([f"{col['name']} ({col['type']})" for col in schema])
                        context += f"  - {table}: {columns}\n"
                    except:
                        context += f"  - {table}\n"
                
                return context
            else:
                # List all databases
                databases = self.introspector.list_databases()
                context = f"\nAvailable databases: {', '.join(databases)}\n"
                return context
        except Exception as e:
            return f"\nDatabase context unavailable: {str(e)}\n"
    
    def build_ambiguity_prompt(
        self,
        user_query: str,
        possible_matches: List[Dict[str, Any]]
    ) -> str:
        """Build a prompt for resolving ambiguity"""
        matches_text = "\n".join([
            f"- Database: {m['database']}, Table: {m['table']}"
            for m in possible_matches
        ])
        
        return f"""The table name in your query is ambiguous. Which one do you mean?

{matches_text}

Please specify the database name, or I'll choose the first one.
"""
    
    def build_schema_prompt(self, table_name: str, database: Optional[str] = None) -> str:
        """Build a prompt to get table schema"""
        try:
            schema = self.introspector.get_table_schema(table_name, database)
            columns = "\n".join([
                f"- {col['name']}: {col['type']} {'(nullable)' if col['nullable'] else '(not null)'}"
                for col in schema
            ])
            return f"Schema for {table_name}:\n{columns}"
        except Exception as e:
            return f"Could not get schema: {str(e)}"


# Singleton instance
prompt_builder = PromptBuilder()
