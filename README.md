# EmendoAI 🤖

**Natural Language to SQL Agent** - Powered by Claude + PostgreSQL

Transform your plain English queries into SQL, execute them against your PostgreSQL database, and get results back in natural language!

## ✨ Features

- **Natural Language → SQL**: Simply ask questions in plain English
- **Database Introspection**: Lists databases, tables, and schemas automatically
- **Smart Execution**: Auto-executes safe queries, asks permission for destructive ones
- **SQL Injection Protection**: Built-in security to protect your database
- **Result Summarization**: Query results returned in easy-to-understand English
- **Intelligent Error Handling**: Automatic SQL repair when errors occur
- **Database Context Switching**: Seamlessly switch between databases

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DEFAULT_DB=postgres

# Anthropic Claude
ANTHROPIC_API_KEY=your_api_key
```

### 3. Run the Agent

**Interactive Mode:**

```bash
python main.py --interactive
```

**Single Query:**

```bash
python main.py --query "List all databases"
```

## 📖 Usage Examples

### List Databases

```
🗣️  You: List all databases
💬  EmendoAI: Available databases: postgres, myapp, analytics
```

### Switch Database

```
🗣️  You: use myapp
💬  EmendoAI: Switched to database: myapp
```

### Show Tables

```
🗣️  You: Show tables in myapp
💬  EmendoAI: Tables in myapp: users, orders, products
```

### Get Table Schema

```
🗣️  You: show schema users
💬  EmendoAI: Schema for 'users':
   - id: integer (NOT NULL)
   - name: character varying (NOT NULL)
   - email: character varying (NOT NULL)
   - created_at: timestamp without time zone (NOT NULL)
```

### Query Data

```
🗣️  You: Get all users from New York
💬  EmendoAI: Found 3 results:
   1. id: 1, name: John Doe, city: New York
   2. id: 5, name: Jane Smith, city: New York
   3. id: 12, name: Bob Wilson, city: New York
```

### Intelligent Error Recovery

When a query fails due to a typo or non-existent table, EmendoAI automatically:

1. Analyzes the PostgreSQL error message
2. Attempts to repair the SQL using the LLM
3. Retries with the corrected query
4. Provides helpful suggestions if repair fails

```
🗣️  You: Select * from usres
💬  EmendoAI: _Note: SQL was automatically corrected_
   Found 5 results: ...
```

## 🔒 Safety Rules

| Query Type       | Behavior                              |
| ---------------- | ------------------------------------- |
| SELECT           | ✅ Auto-execute                       |
| INSERT           | ✅ Auto-execute                       |
| CREATE           | ✅ Auto-execute                       |
| UPDATE           | ⚠️ Shows rows affected, then executes |
| DELETE (≤5 rows) | ✅ Auto-execute                       |
| DELETE (>5 rows) | ❌ Asks permission first              |
| DROP TABLE       | ❌ Asks permission first              |
| DROP DATABASE    | ❌ Blocked (security)                 |

## 🛠️ Configuration

| Variable                   | Default                    | Description                       |
| -------------------------- | -------------------------- | --------------------------------- |
| `POSTGRES_HOST`            | localhost                  | Database host                     |
| `POSTGRES_PORT`            | 5432                       | Database port                     |
| `POSTGRES_USER`            | postgres                   | Database user                     |
| `POSTGRES_PASSWORD`        | -                          | Database password                 |
| `POSTGRES_DEFAULT_DB`      | postgres                   | Default database                  |
| `ANTHROPIC_API_KEY`        | -                          | Your Anthropic API key            |
| `ANTHROPIC_MODEL`          | claude-3-5-sonnet-20241022 | Claude model                      |
| `MAX_TOKENS`               | 4096                       | Max response tokens               |
| `DELETE_ROWS_THRESHOLD`    | 5                          | Rows threshold for permission     |
| `MAX_GENERATION_RETRIES`   | 3                          | Max SQL generation retries        |
| `MAX_EXECUTION_RETRIES`    | 3                          | Max execution retries             |
| `ENABLE_INTELLIGENT_RETRY` | true                       | Enable intelligent error recovery |
| `SUGGEST_AVAILABLE_TABLES` | true                       | Suggest tables on error           |
| `LOG_SQL_REPAIRS`          | true                       | Log SQL repairs                   |

## 🎯 Special Commands

In interactive mode:

- `list databases` or `show databases` - List all databases
- `use <database>` - Switch to a specific database
- `list tables` or `show tables` - List tables in current database
- `show schema <table>` - Show schema for a table
- `show schema for <table>` - Show schema for a table
- `describe <table>` - Show schema for a table

## 🔧 Intelligent Error Handling

EmendoAI now intelligently handles SQL execution errors:

### Error Types Handled

- **relation does not exist**: Automatically suggests available tables
- **column does not exist**: Suggests column corrections
- **syntax error**: Uses LLM to repair the SQL

### How It Works

1. Query execution fails
2. Error is classified (relation, column, syntax)
3. If retryable, the original SQL + error is sent to LLM
4. LLM generates corrected SQL
5. Query retries with corrected SQL
6. User sees "SQL was automatically corrected" note

### Configuration

Control intelligent retry behavior via environment variables:

```env
ENABLE_INTELLIGENT_RETRY=true
SUGGEST_AVAILABLE_TABLES=true
LOG_SQL_REPAIRS=true
MAX_EXECUTION_RETRIES=3
```

## 📁 Project Structure

```
EmendoAI/
├── main.py                    # CLI entry point
├── requirements.txt           # Python dependencies
├── .env.example              # Environment template
├── config/
│   └── settings.py           # Configuration
└── src/
    ├── database/
    │   ├── connection.py     # PostgreSQL connection pool + switching
    │   ├── introspection.py # Schema introspection
    │   └── executor.py       # Query executor + intelligent error handling
    ├── llm/
    │   ├── anthropic_client.py # Claude API client
    │   ├── bedrock_client.py   # AWS Bedrock client
    │   └── cli_client.py       # Claude CLI client
    ├── sql/
    │   └── validator.py      # sqlglot validation
    └── agent/
        ├── agent.py          # Main agent logic
        ├── prompt_builder.py # Prompt generation
        ├── safety.py         # Safety checks
        └── response_parser.py # Result formatting + schema parsing
```

## 🔧 Development

### Running Tests

```bash
# Test database connection
python -c "from src.database.connection import db_connection; print(db_connection.test_connection())"
```

### Adding New Features

The agent is modular - you can extend:

- `src/agent/prompt_builder.py` - Customize LLM prompts
- `src/agent/safety.py` - Add new safety rules
- `src/sql/validator.py` - Add more SQL validations
- `src/database/executor.py` - Add new error handling logic

## 📝 License

MIT License

## 🤝 Contributing

Contributions welcome! Please open an issue or submit a PR.

---

Made with ❤️ using Claude + PostgreSQL
