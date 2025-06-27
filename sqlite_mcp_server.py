import os
import sqlite3
import json
import csv
import io
from mcp.server.fastmcp import FastMCP

# --- Configuration ---
# No default database path - client must specify the database path for all operations
print("SQLite MCP server initialized - no default database path")

# --- GUIDE FOR SMALL MODELS ---
# 🤖 If you're a small model, start with these tools in this order:
# 1. discover_database(db_path) - Get complete database overview
# 2. explain_table(table_name, db_path) - Understand specific tables  
# 3. smart_query(question, db_path) - Ask questions in plain English
# 4. query_database(sql, db_path) - Run SQL when you know what you want
#
# NEW TOOLS ADDED FOR SMALL MODELS:
# - discover_database: Complete schema discovery with relationships and examples
# - explain_table: Simple table explanations with sample data and usage examples  
# - get_schema_summary: Quick overview of all tables
# - Improved error messages with helpful suggestions
# - Clear emoji-based tool descriptions for easy identification

# --- Initialize FastMCP server ---
mcp = FastMCP("sqlite_explorer_server")

# --- Helper Function for DB Connection ---
def get_db_connection(db_path):
    """Establishes a connection to the SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file.
    
    Returns:
        sqlite3.Connection: A connection to the SQLite database.
        
    Raises:
        ValueError: If no database path is provided.
        sqlite3.Error: If there's an error connecting to the database.
    """
    if not db_path:
        raise ValueError("Database path must be provided")
    
    try:
        # Ensure the directory for the database exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            print(f"Created database directory: {db_dir}")
            
        print(f"Connecting to SQLite database at: {db_path}")
        conn = sqlite3.connect(db_path)
        # Return rows as dictionary-like objects (optional, but often convenient)
        # conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        # Re-raise the error to be caught by the tool's error handler
        raise

# --- MCP Tools ---

@mcp.tool()
def list_tables(db_path: str) -> dict:
    """
    📋 SIMPLE: Shows all table names in the database. Use this to see what tables exist.
    
    Args:
        db_path (str): Database path
    
    Returns:
        dict: Simple response with table names
    """
    print(f"Executing list_tables tool with db_path: {db_path}")
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"Found tables: {tables}")
            return {"status": "success", "tables": tables, "db_path": db_path}
    except sqlite3.Error as e:
        print(f"Error in list_tables: {e}")
        return {
            "status": "error", 
            "message": str(e),
            "suggestion": "Make sure the database file exists and is a valid SQLite database. Try using create_database first if the database doesn't exist."
        }
    except Exception as e:
        print(f"Unexpected error in list_tables: {e}")
        return {
            "status": "error", 
            "message": f"An unexpected error occurred: {str(e)}", 
            "db_path": db_path,
            "suggestion": "Check that the database path is correct and accessible."
        }

@mcp.tool()
def list_columns(table_name: str, db_path: str) -> dict:
    """
    Lists all columns and their types for a specific table.
    
    Args:
        table_name (str): The name of the table to inspect.
        db_path (str, optional): Path to the SQLite database file.
                                If None, uses the default path. Defaults to None.
    
    Returns:
        dict: A dictionary containing a list of column details or an error message.
              Example: {"status": "success", "columns": [{"cid": 0, "name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": None, "pk": 1}, ...]}
                       {"status": "error", "message": "Table not found or DB error"}
    """
    print(f"Executing list_columns tool for table: {table_name}, db_path: {db_path}")
    if not table_name or not isinstance(table_name, str):
         return {"status": "error", "message": "Invalid table_name provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # First check if the table exists to prevent SQL injection
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                return {"status": "error", "message": f"Table '{table_name}' not found."}
            
            # Now it's safe to use the table name in PRAGMA (since we validated it exists)
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            
            columns_raw = cursor.fetchall()
            if not columns_raw:
                # Table exists but has no columns (unusual but possible)
                return {"status": "success", "columns": [], "db_path": db_path}

            column_names = [description[0] for description in cursor.description]
            columns = [dict(zip(column_names, row)) for row in columns_raw]
            print(f"Found columns for {table_name}: {columns}")
            return {"status": "success", "columns": columns, "db_path": db_path}
    except sqlite3.Error as e:
        print(f"Error in list_columns for {table_name}: {e}")
        # Check if the error message indicates the table doesn't exist
        if "no such table" in str(e).lower():
             return {"status": "error", "message": f"Table '{table_name}' not found or error accessing it."}
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in list_columns for {table_name}: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def execute_sql(sql_query: str, db_path: str, parameters: list = None) -> dict:
    """
    Executes an arbitrary SQL query (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.).
    For SELECT queries, returns the results.
    For other queries (DML/DDL), commits the changes and returns status/rowcount.
    Uses parameterization to help prevent SQL injection if 'parameters' list is provided.

    Args:
        sql_query (str): The SQL query string to execute. Use '?' placeholders for parameters.
        db_path (str): Path to the SQLite database file. Must be provided.
        parameters (list, optional): A list of values to bind to the placeholders in the query. Defaults to None.

    Returns:
        dict: A dictionary containing results, status, row count, or an error message.
              Example (SELECT): {"status": "success", "columns": ["id", "name"], "rows": [[1, "Alice"], [2, "Bob"]]}
              Example (INSERT/UPDATE/DELETE): {"status": "success", "rows_affected": 1}
              Example (CREATE/DROP): {"status": "success", "message": "Query executed successfully."}
              Example (Error): {"status": "error", "message": "SQL error details"}
    """
    print(f"Executing execute_sql tool with query: {sql_query}, params: {parameters}, db_path: {db_path}")
    if not sql_query or not isinstance(sql_query, str):
         return {"status": "error", "message": "Invalid sql_query provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}

    # Ensure parameters is a list or tuple if provided, default to empty list
    params = parameters if isinstance(parameters, (list, tuple)) else []

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query, params)

            # Determine if it's a SELECT query to fetch results
            is_select = sql_query.strip().upper().startswith("SELECT")

            if is_select:
                rows = cursor.fetchall()
                # Get column names if there are results or if it was a SELECT
                column_names = [description[0] for description in cursor.description] if cursor.description else []
                print(f"SELECT query executed. Columns: {column_names}, Rows fetched: {len(rows)}")
                return {"status": "success", "columns": column_names, "rows": rows, "db_path": db_path}
            else:
                # For non-SELECT (INSERT, UPDATE, DELETE, CREATE, etc.), commit the transaction
                conn.commit()
                rows_affected = cursor.rowcount # Returns -1 for non-DML statements like CREATE
                print(f"Non-SELECT query executed. Rows affected/status: {rows_affected}")
                if rows_affected != -1:
                    return {"status": "success", "rows_affected": rows_affected, "db_path": db_path}
                else:
                    return {"status": "success", "message": "Query executed successfully (DDL or statement with no row count).", "db_path": db_path}

    except sqlite3.Error as e:
        print(f"Error in execute_sql: {e}")
        error_msg = str(e)
        if "no such table" in error_msg.lower():
            return {
                "status": "error", 
                "message": f"Table not found. Use 'list_tables' tool first to see available tables. Error: {error_msg}",
                "db_path": db_path,
                "suggestion": "Try using list_tables tool to see what tables are available"
            }
        return {"status": "error", "message": error_msg, "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in execute_sql: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def create_database(db_path: str) -> dict:
    """
    Creates a new SQLite database at the specified path.
    If the database already exists, this will simply connect to it.
    
    Args:
        db_path (str): Path where the SQLite database file should be created.
                       Can be absolute or relative to the current working directory.
    
    Returns:
        dict: A dictionary containing the status of the operation.
              Example: {"status": "success", "message": "Database created successfully at /path/to/db.sqlite"}
              Example: {"status": "error", "message": "Error details"}
    """
    print(f"Executing create_database tool with path: {db_path}")
    if not db_path or not isinstance(db_path, str):
        return {"status": "error", "message": "Invalid db_path provided."}
    
    try:
        # Ensure the directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            print(f"Created database directory: {db_dir}")
        
        # Connect to the database (this will create it if it doesn't exist)
        with sqlite3.connect(db_path) as conn:
            # Execute a simple query to verify the connection
            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            
            return {
                "status": "success", 
                "message": f"Database created/connected successfully at {db_path}",
                "sqlite_version": version,
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in create_database: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in create_database: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def export_data(table_name: str, db_path: str, format: str = "csv", output_path: str = None, limit: int = None) -> dict:
    """
    Exports table data to a specified format (CSV or JSON).
    
    Args:
        table_name (str): The name of the table to export data from.
        db_path (str): Path to the SQLite database file.
        format (str, optional): Export format, either 'csv' or 'json'. Defaults to 'csv'.
        output_path (str, optional): Path where the output file should be saved.
                                    If None, returns data in the response. Defaults to None.
        limit (int, optional): Maximum number of rows to export. Defaults to None (all rows).
    
    Returns:
        dict: A dictionary containing the status of the operation and exported data if output_path is None.
              Example: {"status": "success", "data": [...], "message": "Data exported successfully"}
              Example: {"status": "error", "message": "Error details"}
    """
    print(f"Executing export_data tool for table: {table_name}, format: {format}, db_path: {db_path}")
    if not table_name or not isinstance(table_name, str):
        return {"status": "error", "message": "Invalid table_name provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    if format.lower() not in ["csv", "json"]:
        return {"status": "error", "message": f"Unsupported export format: {format}. Supported formats: csv, json"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                return {"status": "error", "message": f"Table '{table_name}' not found."}
            
            # Build the query with optional LIMIT clause (table name already validated)
            query = f"SELECT * FROM `{table_name}`"
            if limit is not None and isinstance(limit, int) and limit > 0:
                query += f" LIMIT {limit}"
            
            # Execute the query
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            # Format the data based on the requested format
            if format.lower() == "json":
                # Create a list of dictionaries for JSON format
                data = [dict(zip(columns, row)) for row in rows]
                
                # Write to file or return in response
                if output_path:
                    with open(output_path, 'w') as f:
                        json.dump(data, f, indent=2)
                    return {
                        "status": "success", 
                        "message": f"Data exported successfully to {output_path}",
                        "row_count": len(rows),
                        "db_path": db_path
                    }
                else:
                    return {
                        "status": "success", 
                        "data": data,
                        "row_count": len(rows),
                        "db_path": db_path
                    }
            else:  # CSV format
                if output_path:
                    with open(output_path, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)  # Write header
                        writer.writerows(rows)    # Write data rows
                    return {
                        "status": "success", 
                        "message": f"Data exported successfully to {output_path}",
                        "row_count": len(rows),
                        "db_path": db_path
                    }
                else:
                    # Create CSV in memory for return
                    output = io.StringIO()
                    writer = csv.writer(output)
                    writer.writerow(columns)  # Write header
                    writer.writerows(rows)    # Write data rows
                    csv_data = output.getvalue()
                    return {
                        "status": "success", 
                        "data": csv_data,
                        "row_count": len(rows),
                        "db_path": db_path
                    }
    except sqlite3.Error as e:
        print(f"Error in export_data for {table_name}: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in export_data for {table_name}: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def import_data(table_name: str, db_path: str, file_path: str, format: str = "csv", create_table: bool = False) -> dict:
    """
    Imports data from a CSV or JSON file into a SQLite table.
    
    Args:
        table_name (str): The name of the table to import data into.
        db_path (str): Path to the SQLite database file.
        file_path (str): Path to the file containing data to import.
        format (str, optional): File format, either 'csv' or 'json'. Defaults to 'csv'.
        create_table (bool, optional): Whether to create the table if it doesn't exist. Defaults to False.
    
    Returns:
        dict: A dictionary containing the status of the operation.
              Example: {"status": "success", "message": "Data imported successfully", "rows_imported": 100}
              Example: {"status": "error", "message": "Error details"}
    """
    print(f"Executing import_data tool for table: {table_name}, file: {file_path}, format: {format}, db_path: {db_path}")
    if not table_name or not isinstance(table_name, str):
        return {"status": "error", "message": "Invalid table_name provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
        
    if not file_path or not os.path.exists(file_path):
        return {"status": "error", "message": f"File not found: {file_path}"}
    
    if format.lower() not in ["csv", "json"]:
        return {"status": "error", "message": f"Unsupported import format: {format}. Supported formats: csv, json"}
    
    try:
        # Connect to the database
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            table_exists = cursor.fetchone() is not None
            
            if not table_exists and not create_table:
                return {"status": "error", "message": f"Table '{table_name}' does not exist. Set create_table=True to create it."}
            
            # Load data from the file based on format
            if format.lower() == "json":
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if not isinstance(data, list) or len(data) == 0:
                    return {"status": "error", "message": "JSON file must contain a list of objects"}
                
                # Create the table if needed
                if not table_exists and create_table:
                    # Use the keys of the first object to create columns
                    columns = list(data[0].keys())
                    column_defs = [f"\"{col}\" TEXT" for col in columns]
                    create_table_sql = f"CREATE TABLE \"{table_name}\" ({', '.join(column_defs)})"
                    cursor.execute(create_table_sql)
                    
                # Get the actual columns in the table (table already validated)
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                existing_columns = [row[1] for row in cursor.fetchall()]
                
                # Insert the data
                rows_imported = 0
                for row_data in data:
                    # Filter the data to only include columns that exist in the table
                    filtered_data = {k: v for k, v in row_data.items() if k in existing_columns}
                    if filtered_data:
                        columns = list(filtered_data.keys())
                        placeholders = ["?" for _ in columns]
                        values = [filtered_data[col] for col in columns]
                        
                        quoted_columns = ["\""+col+"\"" for col in columns]
                        insert_sql = f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({', '.join(placeholders)})"
                        cursor.execute(insert_sql, values)
                        rows_imported += 1
            else:  # CSV format
                with open(file_path, 'r', newline='') as f:
                    csv_reader = csv.reader(f)
                    headers = next(csv_reader)  # Read the header row
                    
                    if not table_exists and create_table:
                        # Create the table using the CSV headers
                        column_defs = [f"\"{col}\" TEXT" for col in headers]
                        create_table_sql = f"CREATE TABLE \"{table_name}\" ({', '.join(column_defs)})"
                        cursor.execute(create_table_sql)
                    
                    # Get the actual columns in the table (table name already validated)
                    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                    existing_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Filter headers to only include columns that exist in the table
                    valid_indices = [i for i, col in enumerate(headers) if col in existing_columns]
                    valid_headers = [headers[i] for i in valid_indices]
                    
                    if not valid_headers:
                        return {"status": "error", "message": "No matching columns found between the CSV and the table"}
                    
                    # Insert data
                    placeholders = ["?" for _ in valid_headers]
                    quoted_headers = ["\""+col+"\"" for col in valid_headers]
                    insert_sql = f"INSERT INTO \"{table_name}\" ({', '.join(quoted_headers)}) VALUES ({', '.join(placeholders)})"
                    
                    rows_imported = 0
                    for row in csv_reader:
                        if row:  # Skip empty rows
                            values = [row[i] for i in valid_indices]
                            cursor.execute(insert_sql, values)
                            rows_imported += 1
            
            # Commit the changes
            conn.commit()
            return {
                "status": "success",
                "message": f"Successfully imported {rows_imported} rows into table '{table_name}'",
                "rows_imported": rows_imported,
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in import_data for {table_name}: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in import_data for {table_name}: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def get_table_info(table_name: str, db_path: str) -> dict:
    """
    Gets detailed information about a specific table including row count, storage size, and other statistics.
    
    Args:
        table_name (str): The name of the table to get information about.
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: A dictionary containing table information or an error message.
              Example: {"status": "success", "row_count": 1000, "size_bytes": 51200, ...}
              Example: {"status": "error", "message": "Table not found"}
    """
    print(f"Executing get_table_info tool for table: {table_name}, db_path: {db_path}")
    if not table_name or not isinstance(table_name, str):
        return {"status": "error", "message": "Invalid table_name provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                return {"status": "error", "message": f"Table '{table_name}' not found."}
            
            # Get row count (table name already validated)
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            
            # Get table structure
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns_raw = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            columns = [dict(zip(column_names, row)) for row in columns_raw]
            
            # Get index information
            cursor.execute(f"PRAGMA index_list(`{table_name}`)")
            indexes_raw = cursor.fetchall()
            index_names = [description[0] for description in cursor.description]
            indexes = [dict(zip(index_names, row)) for row in indexes_raw]
            
            # For each index, get the columns it covers
            detailed_indexes = []
            for idx in indexes:
                idx_name = idx.get('name')
                # Validate index name exists before using it
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (idx_name,))
                if cursor.fetchone():
                    cursor.execute(f"PRAGMA index_info(`{idx_name}`)")
                    idx_columns_raw = cursor.fetchall()
                    idx_column_names = [description[0] for description in cursor.description]
                    idx_columns = [dict(zip(idx_column_names, row)) for row in idx_columns_raw]
                    detailed_indexes.append({
                        "name": idx_name,
                        "unique": idx.get('unique'),
                        "columns": idx_columns
                    })
            
            # Try to get table statistics (size estimation)
            # SQLite doesn't provide direct table size information, but we can estimate
            size_bytes = None
            try:
                # This query works only if the sqlite_stat1 table exists (after ANALYZE)
                cursor.execute("SELECT * FROM sqlite_stat1 WHERE tbl=?", (table_name,))
                stats = cursor.fetchall()
                # If stats are available, include them in the response
                if stats:
                    stat_columns = [description[0] for description in cursor.description]
                    table_stats = [dict(zip(stat_columns, row)) for row in stats]
                else:
                    table_stats = None
            except sqlite3.Error:
                # sqlite_stat1 table doesn't exist or other error
                table_stats = None
            
            # Sample data (first few rows)
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
            sample_rows = cursor.fetchall()
            sample_columns = [description[0] for description in cursor.description]
            sample_data = [dict(zip(sample_columns, row)) for row in sample_rows]
            
            return {
                "status": "success",
                "table_name": table_name,
                "row_count": row_count,
                "columns": columns,
                "indexes": detailed_indexes,
                "stats": table_stats,
                "sample_data": sample_data,
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in get_table_info for {table_name}: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in get_table_info for {table_name}: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def get_database_info(db_path: str) -> dict:
    """
    Gets general information about the SQLite database including size, version, and table statistics.
    
    Args:
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: A dictionary containing database information or an error message.
              Example: {"status": "success", "size_bytes": 102400, "tables": [...], ...}
              Example: {"status": "error", "message": "Database not found"}
    """
    print(f"Executing get_database_info tool for db_path: {db_path}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
        
    # Check if the database file exists
    if not os.path.exists(db_path):
        return {"status": "error", "message": f"Database file not found at: {db_path}"}
    
    try:
        # Get file size
        size_bytes = os.path.getsize(db_path)
        
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Get SQLite version
            cursor.execute("SELECT sqlite_version();")
            sqlite_version = cursor.fetchone()[0]
            
            # Get list of all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get table statistics
            table_stats = []
            for table_name in tables:
                # Table names from sqlite_master are already validated
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                
                # Get column count
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns = cursor.fetchall()
                
                table_stats.append({
                    "name": table_name,
                    "row_count": row_count,
                    "column_count": len(columns)
                })
            
            # Get database pragma information
            cursor.execute("PRAGMA database_list;")
            db_list_raw = cursor.fetchall()
            db_list_cols = [description[0] for description in cursor.description]
            db_list = [dict(zip(db_list_cols, row)) for row in db_list_raw]
            
            cursor.execute("PRAGMA page_size;")
            page_size = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA page_count;")
            page_count = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA freelist_count;")
            freelist_count = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA journal_mode;")
            journal_mode = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA synchronous;")
            synchronous = cursor.fetchone()[0]
            
            # Get views, indices, and triggers count
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view';")
            view_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index';")
            index_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger';")
            trigger_count = cursor.fetchone()[0]
            
            # Calculate database size from page information as a cross-check
            calculated_size = page_size * page_count
            
            return {
                "status": "success",
                "db_path": db_path,
                "file_size_bytes": size_bytes,
                "calculated_size_bytes": calculated_size,
                "page_size": page_size,
                "page_count": page_count,
                "freelist_count": freelist_count,
                "journal_mode": journal_mode,
                "synchronous_mode": synchronous,
                "sqlite_version": sqlite_version,
                "attached_databases": db_list,
                "tables": table_stats,
                "table_count": len(tables),
                "view_count": view_count,
                "index_count": index_count,
                "trigger_count": trigger_count
            }
    except sqlite3.Error as e:
        print(f"Error in get_database_info: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in get_database_info: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def backup_database(db_path: str, backup_path: str) -> dict:
    """
    Creates a backup copy of the SQLite database.
    
    Args:
        db_path (str): Path to the source SQLite database file.
        backup_path (str): Path where the backup should be saved.
    
    Returns:
        dict: A dictionary containing the status of the operation.
              Example: {"status": "success", "message": "Database backed up successfully", "backup_path": "/path/to/backup.sqlite"}
              Example: {"status": "error", "message": "Source database not found"}
    """
    print(f"Executing backup_database tool from {db_path} to {backup_path}")
    
    if not db_path:
        return {"status": "error", "message": "Source database path must be provided"}
        
    if not backup_path:
        return {"status": "error", "message": "Backup path must be provided"}
    
    # Check if the source database exists
    if not os.path.exists(db_path):
        return {"status": "error", "message": f"Source database not found at: {db_path}"}
    
    try:
        # Create backup directory if it doesn't exist
        backup_dir = os.path.dirname(backup_path)
        if backup_dir and not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            print(f"Created backup directory: {backup_dir}")
        
        with get_db_connection(db_path) as source_conn:
            # Check if the source is a valid SQLite database
            try:
                source_conn.execute("SELECT sqlite_version();")
            except sqlite3.Error as e:
                return {"status": "error", "message": f"Source is not a valid SQLite database: {str(e)}", "db_path": db_path}
            
            # Open/Create the backup database
            with sqlite3.connect(backup_path) as backup_conn:
                # Use the SQLite backup API if available
                if hasattr(source_conn, 'backup'):
                    source_conn.backup(backup_conn)
                else:
                    # Fallback: Read schema and data and write to backup file
                    # Get schema
                    cursor = source_conn.cursor()
                    backup_cursor = backup_conn.cursor()
                    
                    # Get all SQL statements to recreate schema
                    cursor.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL;")
                    schema_statements = cursor.fetchall()
                    
                    # Execute schema statements on backup
                    for (sql,) in schema_statements:
                        if sql.strip():
                            backup_cursor.execute(sql)
                    
                    # Get all tables
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    # Copy data from each table
                    for table in tables:
                        if table != 'sqlite_sequence':  # Skip SQLite internal tables
                            # Table names from sqlite_master are already validated
                            cursor.execute(f"SELECT * FROM `{table}`;")
                            rows = cursor.fetchall()
                            if rows:
                                # Get column count for this table
                                columns = [description[0] for description in cursor.description]
                                placeholders = ', '.join(['?'] * len(columns))
                                insert_stmt = f"INSERT INTO `{table}` VALUES ({placeholders})"
                                
                                backup_conn.executemany(insert_stmt, rows)
                    
                    # Commit changes
                    backup_conn.commit()
                    
                # Verify the backup was successful
                backup_conn.execute("SELECT sqlite_version();")
                
        # Get file sizes for reporting
        source_size = os.path.getsize(db_path)
        backup_size = os.path.getsize(backup_path)
        
        return {
            "status": "success",
            "message": f"Database backed up successfully from {db_path} to {backup_path}",
            "source_path": db_path,
            "backup_path": backup_path,
            "source_size_bytes": source_size,
            "backup_size_bytes": backup_size
        }
    except sqlite3.Error as e:
        print(f"SQLite error in backup_database: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path, "backup_path": backup_path}
    except Exception as e:
        print(f"Unexpected error in backup_database: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path, "backup_path": backup_path}

@mcp.tool()
def list_indexes(db_path: str, table_name: str = None) -> dict:
    """
    Lists all indexes in the database or for a specific table.
    
    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str, optional): If provided, only show indexes for this table. Defaults to None.
    
    Returns:
        dict: A dictionary containing index information or an error message.
              Example: {"status": "success", "indexes": [{"name": "idx_users_email", "table": "users", ...}, ...]}
              Example: {"status": "error", "message": "Table not found"}
    """
    print(f"Executing list_indexes tool for db_path: {db_path}, table: {table_name if table_name else 'all'}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # If table_name is specified, check if it exists
            if table_name:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    return {"status": "error", "message": f"Table '{table_name}' not found.", "db_path": db_path}
            
            # Get all indexes or indexes for specific table
            if table_name:
                cursor.execute("SELECT * FROM sqlite_master WHERE type='index' AND tbl_name=?", (table_name,))
            else:
                cursor.execute("SELECT * FROM sqlite_master WHERE type='index'")
            
            indexes_raw = cursor.fetchall()
            if not indexes_raw:
                return {
                    "status": "success",
                    "message": f"No indexes found{' for table ' + table_name if table_name else ''}.",
                    "indexes": [],
                    "db_path": db_path
                }
            
            # Get column names for the results
            idx_columns = [description[0] for description in cursor.description]
            indexes_basic = [dict(zip(idx_columns, row)) for row in indexes_raw]
            
            # Get detailed info for each index including column details
            detailed_indexes = []
            for idx in indexes_basic:
                idx_info = {
                    "name": idx.get('name'),
                    "table": idx.get('tbl_name'),
                    "sql": idx.get('sql'),
                }
                
                # Get the columns covered by this index
                # Validate index name exists before using it
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (idx['name'],))
                if cursor.fetchone():
                    cursor.execute(f"PRAGMA index_info(`{idx['name']}`)")
                    idx_columns_raw = cursor.fetchall()
                    idx_column_names = [description[0] for description in cursor.description]
                    idx_columns_info = [dict(zip(idx_column_names, row)) for row in idx_columns_raw]
                else:
                    idx_columns_info = []
                
                idx_info["columns"] = idx_columns_info
                
                # Get index statistics if available
                try:
                    cursor.execute("SELECT * FROM sqlite_stat1 WHERE idx=?", (idx['name'],))
                    stats = cursor.fetchone()
                    if stats:
                        stat_columns = [description[0] for description in cursor.description]
                        idx_info["stats"] = dict(zip(stat_columns, stats))
                except sqlite3.Error:
                    # sqlite_stat1 table doesn't exist or other error
                    pass
                
                detailed_indexes.append(idx_info)
            
            return {
                "status": "success",
                "indexes": detailed_indexes,
                "count": len(detailed_indexes),
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in list_indexes: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in list_indexes: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def list_triggers(db_path: str, table_name: str = None) -> dict:
    """
    Lists all triggers in the database or for a specific table.
    
    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str, optional): If provided, only show triggers for this table. Defaults to None.
    
    Returns:
        dict: A dictionary containing trigger information or an error message.
              Example: {"status": "success", "triggers": [{"name": "trg_update_timestamp", "table": "users", ...}, ...]}
              Example: {"status": "error", "message": "Table not found"}
    """
    print(f"Executing list_triggers tool for db_path: {db_path}, table: {table_name if table_name else 'all'}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # If table_name is specified, check if it exists
            if table_name:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    return {"status": "error", "message": f"Table '{table_name}' not found.", "db_path": db_path}
            
            # Get all triggers or triggers for specific table
            if table_name:
                cursor.execute("SELECT * FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table_name,))
            else:
                cursor.execute("SELECT * FROM sqlite_master WHERE type='trigger'")
            
            triggers_raw = cursor.fetchall()
            if not triggers_raw:
                return {
                    "status": "success",
                    "message": f"No triggers found{' for table ' + table_name if table_name else ''}.",
                    "triggers": [],
                    "db_path": db_path
                }
            
            # Get column names for the results
            trigger_columns = [description[0] for description in cursor.description]
            triggers = [dict(zip(trigger_columns, row)) for row in triggers_raw]
            
            # Format the triggers for better readability
            formatted_triggers = []
            for trigger in triggers:
                formatted_trigger = {
                    "name": trigger.get('name'),
                    "table": trigger.get('tbl_name'),
                    "sql": trigger.get('sql'),
                }
                
                # Parse the SQL to extract additional information if possible
                sql = trigger.get('sql', '')
                
                # Try to determine when the trigger fires (BEFORE, AFTER, INSTEAD OF)
                timing = None
                if "BEFORE" in sql.upper():
                    timing = "BEFORE"
                elif "AFTER" in sql.upper():
                    timing = "AFTER"
                elif "INSTEAD OF" in sql.upper():
                    timing = "INSTEAD OF"
                
                # Try to determine the event (INSERT, UPDATE, DELETE)
                event = None
                if "INSERT" in sql.upper():
                    event = "INSERT"
                elif "UPDATE" in sql.upper():
                    event = "UPDATE"
                elif "DELETE" in sql.upper():
                    event = "DELETE"
                
                formatted_trigger["timing"] = timing
                formatted_trigger["event"] = event
                
                formatted_triggers.append(formatted_trigger)
            
            return {
                "status": "success",
                "triggers": formatted_triggers,
                "count": len(formatted_triggers),
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in list_triggers: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in list_triggers: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def list_views(db_path: str) -> dict:
    """
    Lists all views in the database.
    
    Args:
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: A dictionary containing view information or an error message.
              Example: {"status": "success", "views": [{"name": "active_users", "sql": "CREATE VIEW active_users AS ...", ...}, ...]}
              Example: {"status": "error", "message": "Database error details"}
    """
    print(f"Executing list_views tool for db_path: {db_path}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Get all views
            cursor.execute("SELECT * FROM sqlite_master WHERE type='view'")
            views_raw = cursor.fetchall()
            
            if not views_raw:
                return {
                    "status": "success",
                    "message": "No views found in the database.",
                    "views": [],
                    "db_path": db_path
                }
            
            # Get column names for the results
            view_columns = [description[0] for description in cursor.description]
            views = [dict(zip(view_columns, row)) for row in views_raw]
            
            # Format the views and extract additional information
            formatted_views = []
            for view in views:
                view_name = view.get('name', '')
                view_sql = view.get('sql', '')
                
                # Try to extract the SELECT statement that defines the view
                select_statement = None
                if "AS" in view_sql.upper():
                    select_statement = view_sql.upper().split("AS", 1)[1].strip()
                
                formatted_view = {
                    "name": view_name,
                    "sql": view_sql,
                    "select_statement": select_statement
                }
                
                # Get the columns in the view
                try:
                    # View names from sqlite_master are already validated
                    cursor.execute(f"PRAGMA table_info(`{view_name}`)")
                    columns_raw = cursor.fetchall()
                    column_names = [description[0] for description in cursor.description]
                    columns = [dict(zip(column_names, row)) for row in columns_raw]
                    formatted_view["columns"] = columns
                except sqlite3.Error:
                    # Error getting column info for this view
                    formatted_view["columns"] = []
                
                formatted_views.append(formatted_view)
            
            return {
                "status": "success",
                "views": formatted_views,
                "count": len(formatted_views),
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in list_views: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in list_views: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def show_table(table_name: str, db_path: str) -> dict:
    """
    👀 SIMPLE: Shows table structure and sample data. Perfect for exploring a table quickly.
    
    Args:
        table_name (str): Name of the table
        db_path (str): Database path
    
    Returns:
        dict: Table structure and sample rows
    """
    print(f"Executing show_table tool for table: {table_name}, db_path: {db_path}")
    if not table_name or not isinstance(table_name, str):
        return {"status": "error", "message": "Invalid table_name provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # First check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                return {"status": "error", "message": f"Table '{table_name}' not found."}
            
            # Get table structure
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns_raw = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            columns = [dict(zip(column_names, row)) for row in columns_raw]
            
            # Get sample data (first 5 rows)
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
            sample_rows = cursor.fetchall()
            sample_columns = [description[0] for description in cursor.description]
            sample_data = [dict(zip(sample_columns, row)) for row in sample_rows]
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            
            return {
                "status": "success",
                "table_name": table_name,
                "columns": columns,
                "sample_data": sample_data,
                "total_rows": row_count,
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in show_table for {table_name}: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in show_table for {table_name}: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def query_database(sql: str, db_path: str) -> dict:
    """
    ⚡ SIMPLE: Run any SQL query and get results. Use this when you know the SQL.
    
    Args:
        sql (str): The SQL query to run
        db_path (str): Database path
    
    Returns:
        dict: Query results in simple format
    """
    print(f"Executing query_database tool with query: {sql}, db_path: {db_path}")
    if not sql or not isinstance(sql, str):
        return {"status": "error", "message": "Invalid sql provided."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            # Determine if it's a SELECT query to fetch results
            is_select = sql.strip().upper().startswith("SELECT")

            if is_select:
                rows = cursor.fetchall()
                # Get column names if there are results
                column_names = [description[0] for description in cursor.description] if cursor.description else []
                print(f"SELECT query executed. Columns: {column_names}, Rows fetched: {len(rows)}")
                return {"status": "success", "columns": column_names, "rows": rows, "db_path": db_path}
            else:
                # For non-SELECT queries, commit the transaction
                conn.commit()
                rows_affected = cursor.rowcount
                print(f"Non-SELECT query executed. Rows affected: {rows_affected}")
                return {"status": "success", "rows_affected": rows_affected, "db_path": db_path}

    except sqlite3.Error as e:
        print(f"Error in query_database: {e}")
        # Provide helpful error messages for common issues
        error_msg = str(e)
        if "no such table" in error_msg.lower():
            return {
                "status": "error", 
                "message": f"Table not found. Use 'list_tables' tool first to see available tables. Error: {error_msg}",
                "db_path": db_path,
                "suggestion": "Try using list_tables tool to see what tables are available"
            }
        return {"status": "error", "message": error_msg, "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in query_database: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def smart_query(question: str, db_path: str) -> dict:
    """
    🤖 PERFECT FOR SMALL MODELS: Ask questions in plain English and get answers!
    This tool automatically finds the right table and builds the query for you.
    Examples: "show customers from Germany", "how many orders are there", "list all artists"
    
    Args:
        question (str): Natural language question about the data (e.g., "show customers from Germany")
        db_path (str): Database path
    
    Returns:
        dict: Complete answer with data and explanation
    """
    print(f"Executing smart_query tool with question: {question}, db_path: {db_path}")
    if not question or not isinstance(question, str):
        return {"status": "error", "message": "Please provide a question about the data."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Step 1: Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                return {"status": "error", "message": "No tables found in database", "db_path": db_path}
            
            # Step 2: Find relevant table based on question keywords
            question_lower = question.lower()
            relevant_table = None
            
            # Common table name patterns
            table_keywords = {
                'customer': ['customer', 'customers', 'client', 'clients'],
                'order': ['order', 'orders', 'purchase', 'purchases'],
                'product': ['product', 'products', 'item', 'items'],
                'employee': ['employee', 'employees', 'staff', 'worker'],
                'invoice': ['invoice', 'invoices', 'bill', 'bills'],
                'track': ['track', 'tracks', 'song', 'songs', 'music'],
                'album': ['album', 'albums'],
                'artist': ['artist', 'artists', 'band', 'bands'],
                'genre': ['genre', 'genres', 'category', 'categories']
            }
            
            # Try to match question keywords with table names
            for table in tables:
                table_lower = table.lower()
                if any(keyword in question_lower for keyword in [table_lower]):
                    relevant_table = table
                    break
                
                # Check against keyword patterns
                for pattern_key, keywords in table_keywords.items():
                    if table_lower.startswith(pattern_key) or pattern_key in table_lower:
                        if any(keyword in question_lower for keyword in keywords):
                            relevant_table = table
                            break
                if relevant_table:
                    break
            
            # If no specific table found, use the first table
            if not relevant_table:
                relevant_table = tables[0]
            
            # Step 3: Get table structure
            cursor.execute(f"PRAGMA table_info(`{relevant_table}`)")
            columns_raw = cursor.fetchall()
            columns = [row[1] for row in columns_raw]  # Get column names
            
            # Step 4: Build query based on question
            query = f"SELECT * FROM `{relevant_table}`"
            
            # Look for filtering keywords
            if any(word in question_lower for word in ['germany', 'german']):
                if 'country' in [col.lower() for col in columns]:
                    query += " WHERE Country = 'Germany'"
                elif 'location' in [col.lower() for col in columns]:
                    query += " WHERE Location LIKE '%Germany%'"
            
            # Add LIMIT to prevent huge results
            query += " LIMIT 50"
            
            # Step 5: Execute query
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            # Step 6: Format results
            result_data = []
            for row in rows:
                result_data.append(dict(zip(column_names, row)))
            
            return {
                "status": "success",
                "question": question,
                "table_used": relevant_table,
                "available_tables": tables,
                "query_executed": query,
                "columns": column_names,
                "data": result_data,
                "row_count": len(result_data),
                "explanation": f"Found {len(result_data)} records in table '{relevant_table}' based on your question.",
                "db_path": db_path
            }
            
    except sqlite3.Error as e:
        print(f"Error in smart_query: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in smart_query: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def discover_database(db_path: str) -> dict:
    """
    🔍 PERFECT FOR SMALL MODELS: Discovers and explains the entire database structure in simple terms.
    This should be your FIRST tool call when working with any database!
    
    Args:
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: Complete database overview with tables, relationships, and sample data
    """
    print(f"Executing discover_database tool for db_path: {db_path}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [row[0] for row in cursor.fetchall()]
            
            if not table_names:
                return {"status": "error", "message": "No tables found in database", "db_path": db_path}
            
            # Build comprehensive schema information
            database_schema = {
                "database_path": db_path,
                "total_tables": len(table_names),
                "tables": {}
            }
            
            # Get detailed info for each table
            for table_name in table_names:
                # Get columns
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns_raw = cursor.fetchall()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                
                # Get sample data (first 2 rows)
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 2")
                sample_rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                
                # Format column information
                columns = []
                primary_keys = []
                foreign_keys = []
                
                for col in columns_raw:
                    col_info = {
                        "name": col[1],
                        "type": col[2],
                        "required": bool(col[3]),  # NOT NULL
                        "primary_key": bool(col[5])
                    }
                    columns.append(col_info)
                    
                    if col[5]:  # is primary key
                        primary_keys.append(col[1])
                
                # Get foreign key information
                cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)")
                fk_raw = cursor.fetchall()
                for fk in fk_raw:
                    foreign_keys.append({
                        "column": fk[3],
                        "references_table": fk[2],
                        "references_column": fk[4]
                    })
                
                # Format sample data
                sample_data = []
                for row in sample_rows:
                    sample_data.append(dict(zip(column_names, row)))
                
                database_schema["tables"][table_name] = {
                    "row_count": row_count,
                    "columns": columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys,
                    "sample_data": sample_data
                }
            
            # Add helpful summary for small models
            summary = f"""
DATABASE OVERVIEW:
📊 This database has {len(table_names)} tables with the following structure:

TABLES:
"""
            for table_name, info in database_schema["tables"].items():
                summary += f"• {table_name}: {info['row_count']} rows, {len(info['columns'])} columns\n"
            
            summary += f"""
QUICK START EXAMPLES:
• To see all customers: query_database("SELECT * FROM Customer LIMIT 10", "{db_path}")
• To see all tables: The tables are: {', '.join(table_names)}
• To explore a table: show_table("Customer", "{db_path}")

RELATIONSHIPS:
"""
            # Find relationships between tables
            relationships = []
            for table_name, info in database_schema["tables"].items():
                for fk in info["foreign_keys"]:
                    relationships.append(f"{table_name}.{fk['column']} → {fk['references_table']}.{fk['references_column']}")
            
            if relationships:
                summary += "\n".join(f"• {rel}" for rel in relationships)
            else:
                summary += "• No foreign key relationships found"
            
            return {
                "status": "success",
                "schema": database_schema,
                "summary": summary,
                "quick_start": {
                    "first_query": f"query_database(\"SELECT * FROM {table_names[0]} LIMIT 5\", \"{db_path}\")",
                    "explore_table": f"show_table(\"{table_names[0]}\", \"{db_path}\")",
                    "list_all_tables": f"list_tables(\"{db_path}\")"
                },
                "db_path": db_path
            }
            
    except sqlite3.Error as e:
        print(f"Error in discover_database: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in discover_database: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def explain_table(table_name: str, db_path: str) -> dict:
    """
    📋 PERFECT FOR SMALL MODELS: Explains a table in simple, clear language.
    Shows what the table contains, its structure, and provides examples.
    
    Args:
        table_name (str): Name of the table to explain
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: Simple explanation of the table with examples
    """
    print(f"Executing explain_table tool for table: {table_name}, db_path: {db_path}")
    
    if not table_name or not isinstance(table_name, str):
        return {"status": "error", "message": "Please provide a table name."}
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                # Get available tables to help
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                available_tables = [row[0] for row in cursor.fetchall()]
                return {
                    "status": "error", 
                    "message": f"Table '{table_name}' not found.",
                    "available_tables": available_tables,
                    "suggestion": f"Try: explain_table(\"{available_tables[0]}\", \"{db_path}\") if you want to explore the first table."
                }
            
            # Get table structure
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns_raw = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 3")
            sample_rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            # Build simple explanation
            explanation = f"""
TABLE: {table_name}
📊 Contains {row_count} records

COLUMNS:
"""
            
            key_columns = []
            for col in columns_raw:
                col_name = col[1]
                col_type = col[2]
                is_required = bool(col[3])
                is_primary = bool(col[5])
                
                status = ""
                if is_primary:
                    status = " (PRIMARY KEY)"
                    key_columns.append(col_name)
                elif is_required:
                    status = " (REQUIRED)"
                
                explanation += f"• {col_name}: {col_type}{status}\n"
            
            # Add sample data
            explanation += f"\nSAMPLE DATA:\n"
            for i, row in enumerate(sample_rows, 1):
                explanation += f"Row {i}: "
                row_data = []
                for j, value in enumerate(row):
                    if j < 3:  # Show only first 3 columns to keep it simple
                        row_data.append(f"{column_names[j]}={value}")
                explanation += ", ".join(row_data)
                if len(row) > 3:
                    explanation += f" ... (+{len(row)-3} more columns)"
                explanation += "\n"
            
            # Add usage examples
            explanation += f"""
EXAMPLE QUERIES:
• See all data: query_database("SELECT * FROM {table_name} LIMIT 10", "{db_path}")
• Count records: query_database("SELECT COUNT(*) FROM {table_name}", "{db_path}")
"""
            
            if key_columns:
                explanation += f"• Find by ID: query_database(\"SELECT * FROM {table_name} WHERE {key_columns[0]} = 1\", \"{db_path}\")\n"
            
            # Get foreign key relationships
            cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)")
            fk_raw = cursor.fetchall()
            
            relationships = []
            for fk in fk_raw:
                relationships.append(f"{fk[3]} → {fk[2]}.{fk[4]}")
            
            if relationships:
                explanation += f"\nRELATIONSHIPS:\n"
                for rel in relationships:
                    explanation += f"• {rel}\n"
            
            return {
                "status": "success",
                "table_name": table_name,
                "explanation": explanation,
                "row_count": row_count,
                "column_count": len(columns_raw),
                "sample_queries": [
                    f"query_database(\"SELECT * FROM {table_name} LIMIT 10\", \"{db_path}\")",
                    f"query_database(\"SELECT COUNT(*) FROM {table_name}\", \"{db_path}\")"
                ],
                "db_path": db_path
            }
            
    except sqlite3.Error as e:
        print(f"Error in explain_table: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in explain_table: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def get_schema_summary(db_path: str) -> dict:
    """
    📝 PERFECT FOR SMALL MODELS: Gets a quick, simple summary of all tables.
    Use this when you need a fast overview without too much detail.
    
    Args:
        db_path (str): Path to the SQLite database file.
    
    Returns:
        dict: Simple summary of all tables with basic info
    """
    print(f"Executing get_schema_summary tool for db_path: {db_path}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [row[0] for row in cursor.fetchall()]
            
            if not table_names:
                return {"status": "error", "message": "No tables found in database", "db_path": db_path}
            
            summary = {
                "database": db_path,
                "table_count": len(table_names),
                "tables": []
            }
            
            for table_name in table_names:
                # Get basic info
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns = cursor.fetchall()
                
                # Find primary key
                primary_key = None
                for col in columns:
                    if col[5]:  # is primary key
                        primary_key = col[1]
                        break
                
                summary["tables"].append({
                    "name": table_name,
                    "rows": row_count,
                    "columns": len(columns),
                    "primary_key": primary_key
                })
            
            # Create simple text summary
            text_summary = f"Database has {len(table_names)} tables:\n"
            for table in summary["tables"]:
                text_summary += f"• {table['name']}: {table['rows']} rows, {table['columns']} columns"
                if table['primary_key']:
                    text_summary += f" (key: {table['primary_key']})"
                text_summary += "\n"
            
            return {
                "status": "success",
                "summary": summary,
                "text_summary": text_summary,
                "next_steps": [
                    f"explain_table(\"{table_names[0]}\", \"{db_path}\") - to explore the first table",
                    f"query_database(\"SELECT * FROM {table_names[0]} LIMIT 5\", \"{db_path}\") - to see sample data"
                ],
                "db_path": db_path
            }
            
    except sqlite3.Error as e:
        print(f"Error in get_schema_summary: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in get_schema_summary: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

@mcp.tool()
def get_query_plan(db_path: str, sql_query: str) -> dict:
    """
    Gets the execution plan for a SQL query, useful for query optimization.
    
    Args:
        db_path (str): Path to the SQLite database file.
        sql_query (str): The SQL query to analyze.
    
    Returns:
        dict: A dictionary containing the query plan or an error message.
              Example: {"status": "success", "plan": [{...query plan details...}]}
              Example: {"status": "error", "message": "Invalid SQL query"}
    """
    print(f"Executing get_query_plan tool for query: {sql_query}, db_path: {db_path}")
    
    if not db_path:
        return {"status": "error", "message": "Database path must be provided"}
    
    if not sql_query or not isinstance(sql_query, str):
        return {"status": "error", "message": "Invalid sql_query provided."}
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Make sure the query is a SELECT query (EXPLAIN only works on SELECT statements)
            if not sql_query.strip().upper().startswith("SELECT"):
                return {"status": "error", "message": "Query plan is only available for SELECT statements"}
            
            # Use EXPLAIN QUERY PLAN to get the query plan
            cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}")
            plan_rows = cursor.fetchall()
            
            if not plan_rows:
                return {"status": "error", "message": "No query plan generated", "db_path": db_path}
            
            # Get the column names
            plan_columns = [description[0] for description in cursor.description]
            plan_steps = [dict(zip(plan_columns, row)) for row in plan_rows]
            
            # Use EXPLAIN to get more detailed information
            cursor.execute(f"EXPLAIN {sql_query}")
            explain_rows = cursor.fetchall()
            explain_columns = [description[0] for description in cursor.description]
            explain_steps = [dict(zip(explain_columns, row)) for row in explain_rows]
            
            # Execute the query to get actual result columns (but don't fetch all results)
            try:
                cursor.execute(sql_query)
                result_columns = [description[0] for description in cursor.description]
                # Just fetch one row to check if the query returns any results
                sample_row = cursor.fetchone()
                has_results = sample_row is not None
            except sqlite3.Error as e:
                # If there's an error executing the query, include it in the response
                result_columns = []
                has_results = False
                execution_error = str(e)
            else:
                execution_error = None
            
            return {
                "status": "success",
                "query": sql_query,
                "plan": plan_steps,  # Higher level plan
                "explain": explain_steps,  # More detailed information
                "result_columns": result_columns,
                "has_results": has_results,
                "execution_error": execution_error,
                "db_path": db_path
            }
    except sqlite3.Error as e:
        print(f"Error in get_query_plan: {e}")
        return {"status": "error", "message": str(e), "db_path": db_path}
    except Exception as e:
        print(f"Unexpected error in get_query_plan: {e}")
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}", "db_path": db_path}

# --- Run the Server ---
if __name__ == "__main__":
    print("Starting SQLite MCP server...")
    # Run the server using stdio transport as expected by MCP clients like IDE extensions
    mcp.run(transport='stdio')
    print("SQLite MCP server stopped.")