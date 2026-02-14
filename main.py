"""
EmendoAI - Natural Language to SQL Agent
Main entry point with CLI interface
"""
import sys
import argparse
import logging
from typing import Optional

from config import settings
from src.agent.agent import create_agent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_banner():
    """Print welcome banner"""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   ███████╗██████╗  █████╗  ██████╗███████╗██████╗  █████╗   ║
║   ██╔════╝██╔══██╗██╔══██╗██╔════╝██╔════╝██╔══██╗██╔══██╗  ║
║   █████╗  ██████╔╝███████║██║     █████╗  ██████╔╝███████║  ║
║   ██╔══╝  ██╔══██╗██╔══██║██║     ██╔══╝  ██╔══██╗██╔══██║  ║
║   ██║     ██║  ██║██║  ██║╚██████╗███████╗██████╔╝██║  ██║  ║
║   ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚══════╝╚═════╝ ╚═╝  ╚═╝  ║
║                                                               ║
║          Natural Language to SQL Agent                        ║
║          Powered by Claude + PostgreSQL                       ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def interactive_mode(agent):
    """Run agent in interactive mode"""
    print("\nWelcome to EmendoAI! Type 'exit' or 'quit' to stop.\n")
    print("Tip: You can prefix queries with '--database <name>' to specify a database.\n")
    
    current_db = None
    
    while True:
        try:
            user_input = input("\n🗣️  You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['exit', 'quit', 'bye']:
                print("\n👋 Goodbye! Thanks for using EmendoAI!")
                break
            
            # Check for database switch
            if user_input.startswith('--database '):
                db_name = user_input[len('--database '):].strip()
                try:
                    tables = agent.list_tables(db_name)
                    current_db = db_name
                    print(f"\n📂 Switched to database: {db_name}")
                    print(f"   Tables: {', '.join(tables) if tables else 'No tables'}")
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                continue
            
            # List databases command
            if user_input.lower() in ['list databases', 'show databases', 'databases']:
                try:
                    dbs = agent.list_databases()
                    print(f"\n📋 Available databases: {', '.join(dbs)}")
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                continue
            
            # List tables command
            if user_input.lower() in ['list tables', 'show tables', 'tables']:
                try:
                    tables = agent.list_tables(current_db)
                    print(f"\n📋 Tables in {current_db or 'current database'}: {', '.join(tables) if tables else 'No tables'}")
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                continue
            
            # Check for special commands
            if user_input.lower().startswith('show schema '):
                table_name = user_input[len('show schema '):].strip()
                try:
                    schema = agent.get_table_schema(table_name, current_db)
                    print(f"\n📝 Schema for '{table_name}':")
                    for col in schema:
                        nullable = "NULL" if col['nullable'] else "NOT NULL"
                        print(f"   - {col['name']}: {col['type']} ({nullable})")
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                continue
            
            # Process regular query
            print("\n🤔 Processing...")
            
            response, sql = agent.process_query(user_input, current_db)
            
            print(f"\n💬 EmendoAI:")
            print(f"   {response}")
            
            if sql:
                print(f"\n📄 SQL: {sql}")
            
            # Check if we need permission for follow-up
            if "Do you want to proceed?" in response:
                # This was a destructive action requiring permission
                follow_up = input("\n🤔 Do you want to execute this? (yes/no): ").strip().lower()
                
                if follow_up in ['yes', 'y', 'sure', 'go ahead']:
                    print("\n🔄 Executing...")
                    response, sql = agent.process_query(user_input, current_db, user_approved=True)
                    print(f"\n💬 EmendoAI:\n   {response}")
                else:
                    print("\n❌ Query cancelled.")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye! Thanks for using EmendoAI!")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"\n❌ An error occurred: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='EmendoAI - Natural Language to SQL Agent',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --query "List all databases"
  python main.py --query "Show tables in myapp"
  python main.py --query "Get all users from New York"
  python main.py --interactive
        """
    )
    
    parser.add_argument(
        '--query', '-q',
        type=str,
        help='Natural language query to execute'
    )
    
    parser.add_argument(
        '--database', '-d',
        type=str,
        default=None,
        help='Database to use (optional)'
    )
    
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Start interactive mode'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        default=None,
        help='Anthropic API key (optional, can use ANTHROPIC_API_KEY env var)'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    # Check for API key
    api_key = args.api_key or settings.ANTHROPIC_API_KEY
    
    if not api_key:
        print("\n❌ Error: ANTHROPIC_API_KEY is required.")
        print("   Set it via:")
        print("   - Command line: --api-key YOUR_KEY")
        print("   - Environment: export ANTHROPIC_API_KEY=YOUR_KEY")
        print("   - .env file: ANTHROPIC_API_KEY=YOUR_KEY")
        sys.exit(1)
    
    # Create agent
    try:
        agent = create_agent(api_key)
        print("\n✅ Agent initialized successfully!")
    except Exception as e:
        print(f"\n❌ Error initializing agent: {e}")
        sys.exit(1)
    
    # Run in appropriate mode
    if args.interactive or args.query is None:
        interactive_mode(agent)
    else:
        # Single query mode
        try:
            print(f"\n🤔 Processing: {args.query}")
            response, sql = agent.process_query(args.query, args.database)
            
            print(f"\n💬 EmendoAI:")
            print(f"   {response}")
            
            if sql:
                print(f"\n📄 SQL: {sql}")
                
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"\n❌ Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
