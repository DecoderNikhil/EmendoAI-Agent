# EmendoAI 🤖

**Natural Language to SQL Agent** - Powered by Claude + PostgreSQL

Transform your plain English queries into SQL, execute them against your PostgreSQL database, and get results back in natural language!

## ✨ Features

- **Natural Language → SQL**: Simply ask questions in plain English
- **Database Introspection**: Lists databases, tables, and schemas automatically
- **Smart Execution**: Auto-executes safe queries, asks permission for destructive ones
- **SQL Injection Protection**: Built-in security to protect your database
- **Result Summarization**: Query results returned in easy-to-understand English
- **Retry Logic**: Automatic retries to prevent token exhaustion

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

| Variable                 | Default                    | Description                   |
| ------------------------ | -------------------------- | ----------------------------- |
| `POSTGRES_HOST`          | localhost                  | Database host                 |
| `POSTGRES_PORT`          | 5432                       | Database port                 |
| `POSTGRES_USER`          | postgres                   | Database user                 |
| `POSTGRES_PASSWORD`      | -                          | Database password             |
| `POSTGRES_DEFAULT_DB`    | postgres                   | Default database              |
| `ANTHROPIC_API_KEY`      | -                          | Your Anthropic API key        |
| `ANTHROPIC_MODEL`        | claude-3-5-sonnet-20241022 | Claude model                  |
| `MAX_TOKENS`             | 4096                       | Max response tokens           |
| `DELETE_ROWS_THRESHOLD`  | 5                          | Rows threshold for permission |
| `MAX_GENERATION_RETRIES` | 3                          | Max SQL generation retries    |
| `MAX_EXECUTION_RETRIES`  | 2                          | Max execution retries         |

## 🎯 Special Commands

In interactive mode:

- `--database <name>` - Switch to a specific database
- `list databases` or `show databases` - List all databases
- `list tables` or `show tables` - List tables in current database
- `show schema <table>` - Show schema for a table

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
    │   ├── connection.py     # PostgreSQL connection pool
    │   ├── introspection.py # Schema introspection
    │   └── executor.py       # Query executor
    ├── llm/
    │   └── anthropic_client.py # Claude API client
    ├── sql/
    │   └── validator.py      # sqlglot validation
    └── agent/
        ├── agent.py          # Main agent logic
        ├── prompt_builder.py # Prompt generation
        ├── safety.py         # Safety checks
        └── response_parser.py # Result formatting
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

## 📝 License

MIT License

## 🤝 Contributing

Contributions welcome! Please open an issue or submit a PR.

---

Made with ❤️ using Claude + PostgreSQL
