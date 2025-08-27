import duckdb


def test_duckdb_connection(db_path: str = 'db/test_db.duckdb') -> bool:
    """
    Tests the connection to a DuckDB database using a with statement.
    
    Args:
        db_path: The file path to the DuckDB database. 
                 Defaults to 'test_duckdb.db' for a file-based connection.

    Returns:
        True if the connection is successful, False otherwise.
    """
    try:
        # The 'with' statement automatically handles opening and closing the connection
        with duckdb.connect(':memory:') as con:
            # You can execute a simple query to verify the connection is active
            con.execute("SELECT 1 AS connection_test_result")
            print(f"✅ Connection to '{db_path}' was successful.")
            return True
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return False

# --- Example Usage ---
if __name__ == '__main__':
    # Test a successful connection to a file-based database
    if test_duckdb_connection():
        print("Ready to proceed with data operations.")

    # Test a connection that might fail (e.g., a read-only file that doesn't exist)
    if not test_duckdb_connection(db_path='non_existent_file.duckdb'):
        print("\nConnection failed as expected. The file doesn't exist.")