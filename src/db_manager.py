import duckdb
from loguru import logger
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


class DBAdministrator:

    db_path = os.getenv('DB_PATH', ':memory:')

    def test_duckdb_connection(self) -> bool:
        try:
            # The 'with' statement automatically handles opening and closing the connection
            with duckdb.connect(self.db_path) as con:
                # You can execute a simple query to verify the connection is active
                con.execute("SELECT 1 AS connection_test_result")
                logger.debug(f"✅ Connection to '{self.db_path}' was successful.")
                return True
        except Exception as e:
            logger.error(f"❌ An error occurred: {e}")
            return False

    @classmethod
    def create_duckdb_table_from_csv(self, data, file_name: str) -> None:
        
        with duckdb.connect(self.db_path) as conn:
            raw_tbl = conn.read_csv(data, sep=",", encoding='utf-8', null_padding=True, ignore_errors=True)
            query = f'CREATE OR REPLACE TABLE {file_name} AS SELECT * FROM raw_tbl'
            table_shape = conn.sql(query).to_df().shape
            logger.debug(f"Table '{file_name}' created with shape {table_shape}")

    @classmethod
    def create_duckdb_table_from_dataframe(self, data, table_name: str) -> None:

        with duckdb.connect(self.db_path) as conn: 
            conn.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data')
            logger.debug(f"Table '{table_name}' created with shape {data.shape}")

    @classmethod
    def create_duckdb_table_from_excel(self, data_path: str, table_name: str, sheet_name: str='Sheet1') -> None:
        
        with duckdb.connect(self.db_path, read_only=False) as conn:
            # Insert table into duckdb from read file
            conn.sql("INSTALL spatial; LOAD spatial;")
            # conn.execute(f"CREATE SCHEMA IF NOT EXISTS temp;")
            conn.execute(
                f""" CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * 
                            FROM st_read("{data_path}", layer='{sheet_name}');
            """)

    @classmethod
    def get_pandas_from_duckdb_table(self, table_name: str) -> None:
        with duckdb.connect(self.db_path) as conn:
            query = f'SELECT * FROM {table_name}'
            table_ = duckdb.sql(query, connection=conn).to_df()
            logger.debug(f"Table '{table_name}' with {table_.shape} retrieved successfully.")

    @classmethod
    def get_polars_from_duckdb_table(self, table_name: str) -> None:
        with duckdb.connect(self.db_path) as conn:
            query = f'SELECT * FROM {table_name}'
            table_ = conn.sql(query).pl()
            logger.debug(f"Table '{table_name}' with {table_.shape} retrieved successfully.")
        return table_

    @classmethod
    def get_table_list(self) -> list:
        with duckdb.connect(self.db_path) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_list = [table[0] for table in tables]
            logger.debug(f"Tables recorded={len(table_list)} in database - {table_list}")
        return table_list

# Initialize a DBManager instance for testing
db_admin = DBAdministrator()


# --- Example Usage ---
if __name__ == '__main__':
    # Test a successful connection to a file-based database
    if db_admin.test_duckdb_connection():
        logger.info("Ready to proceed with data operations.")

    # Test a connection that might fail (e.g., a read-only file that doesn't exist)
    if not db_admin.test_duckdb_connection(db_path='non_existent_file.duckdb'):
        logger.warning("\nConnection failed as expected. The file doesn't exist.")