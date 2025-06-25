# SQLite MCP Server

A Model Context Protocol (MCP) server that provides comprehensive SQLite database management capabilities. This server allows AI assistants and other MCP clients to interact with SQLite databases through a standardized interface.

## Features

- **Database Management**: Create, backup, and get database information
- **Table Operations**: List tables, get table info, list columns with detailed metadata
- **Data Querying**: Execute arbitrary SQL queries with parameterization support
- **Data Import/Export**: Import and export data in CSV and JSON formats
- **Database Schema**: List indexes, triggers, and views
- **Query Optimization**: Get query execution plans for performance analysis
- **Safety**: Built-in SQL injection protection and error handling

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

### Step 1: Clone the Repository

```bash
git clone https://github.com/ClaudiuJitea/sqlite-mcp-server.git
cd sqlite-mcp-server
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Configure MCP Client

The server includes a sample configuration file (`mcp_config.json`) that you can use with MCP-compatible clients like Claude Desktop or other MCP clients.

For Claude Desktop, add the following to your MCP configuration:

```json
{
  "mcpServers": {
    "sqlite_explorer_server": {
      "command": "python",
      "args": [
        "/path/to/your/sqlite_mcp_server.py"
      ],
      "disabled": false
    }
  }
}
```

Replace `/path/to/your/sqlite_mcp_server.py` with the actual path to the server file.

## Usage

### Running the Server

The server can be run directly for testing:

```bash
python sqlite_mcp_server.py
```

However, it's typically used through an MCP client like Claude Desktop.

### Available Tools

The server provides the following tools:

#### Database Management
- `create_database(db_path)` - Create a new SQLite database
- `get_database_info(db_path)` - Get comprehensive database information
- `backup_database(db_path, backup_path)` - Create a database backup

#### Schema Exploration
- `list_tables(db_path)` - List all tables in the database
- `list_columns(table_name, db_path)` - Get column information for a table
- `get_table_info(table_name, db_path)` - Get detailed table information
- `list_indexes(db_path, table_name=None)` - List database indexes
- `list_triggers(db_path, table_name=None)` - List database triggers
- `list_views(db_path)` - List database views

#### Data Operations
- `execute_sql(sql_query, db_path, parameters=None)` - Execute SQL queries
- `export_data(table_name, db_path, format="csv", output_path=None, limit=None)` - Export table data
- `import_data(table_name, db_path, file_path, format="csv", create_table=False)` - Import data from files

#### Query Analysis
- `get_query_plan(db_path, sql_query)` - Get query execution plan for optimization

### Example Usage with MCP Client

Once configured with an MCP client, you can use natural language to interact with your SQLite databases:

```
"List all tables in my database at ./data/example.db"
"Show me the structure of the users table"
"Execute this SQL query: SELECT * FROM users WHERE age > 25"
"Export the products table to CSV format"
"Create a backup of my database"
```

## Sample Database

The repository includes `Chinook_Sqlite.sqlite`, a sample database you can use for testing. This is a popular sample database that represents a digital media store.

## Security Features

- **Parameterized Queries**: Supports parameter binding to prevent SQL injection
- **Path Validation**: Validates database paths and prevents unauthorized access
- **Error Handling**: Comprehensive error handling with detailed error messages
- **Table Validation**: Validates table existence before operations

## Configuration Options

The server supports various configuration options through the `mcp_config.json` file:

- **command**: Python executable path (use "python" for system Python)
- **args**: Path to the server script
- **disabled**: Whether the server is enabled

## Development

### Project Structure

```
sqlite-mcp-server/
├── sqlite_mcp_server.py    # Main server implementation
├── mcp_config.json         # MCP client configuration
├── requirements.txt        # Python dependencies
├── Chinook_Sqlite.sqlite  # Sample database
└── README.md              # This file
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Troubleshooting

### Common Issues

1. **Server won't start**: Ensure all dependencies are installed and Python path is correct
2. **Database connection errors**: Check that the database path is accessible and valid
3. **Permission errors**: Ensure the server has read/write permissions for the database location

### Debug Mode

The server includes extensive logging. Check the console output for error messages and debugging information.

## License

This project is open source. Please check the repository for license information.

## Support

For issues, questions, or contributions, please visit the GitHub repository at:
https://github.com/ClaudiuJitea/sqlite-mcp-server

## Changelog

### Version 1.0.0
- Initial release with comprehensive SQLite MCP server functionality
- Support for all major SQLite operations
- Import/Export capabilities
- Query optimization tools
- Comprehensive error handling and security features 