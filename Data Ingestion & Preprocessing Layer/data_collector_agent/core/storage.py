# core/storage.py
import logging
import os
import json
import datetime
from typing import Dict
import pandas as pd
from psycopg2 import pool, sql
from psycopg2.extras import execute_values
from core.exceptions import StorageError

logger = logging.getLogger(__name__)


class DataStorage:
    """Handles file-based and CSV storage of collected data."""

    def __init__(self, config: Dict):
        """Initializes the data storage."""
        self.config = config
        self.storage_type = config.get("type", "file")  # Default to file storage

        if self.storage_type in ("file", "csv"):
            self.data_dir = config.get("data_dir", "data")
            os.makedirs(self.data_dir, exist_ok=True)  # Ensure directory exists
        elif self.storage_type == "database":
          logger.warning("Using database storage type, but DataStorage is being instantiated. This might be incorrect.")

    def store_data(self, data: Dict):
        """Stores data based on the configured storage type."""
        if self.storage_type == "file":
            self._store_to_file(data)
        elif self.storage_type == "csv":
            self._store_to_csv(data)
        #  No 'database' case here.  Handled by PostgresDataStorage.
        else:
            raise StorageError(f"Unsupported storage type: {self.storage_type}")

    def _store_to_file(self, data: Dict):
        """Stores data to a JSON file."""
        try:
            source = data.get("source", "unknown")
            data_type = data.get("data_type", "general")

            # Create directory if it doesn't exist
            directory = os.path.join(self.data_dir, source, data_type)
            os.makedirs(directory, exist_ok=True)

            # Create filename based on timestamp, using UTC
            timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
            asset = data.get("asset", "general").replace("/", "-").replace(":", "-")
            filename = f"{timestamp}_{asset}.json"

            # Write data to file
            filepath = os.path.join(directory, filename)
            with open(filepath, 'w') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)  # Use ensure_ascii=False

            logger.debug(f"Data stored to {filepath}")

        except Exception as e:
            raise StorageError(f"Error storing data to file: {e}")


    def _store_to_csv(self, data: Dict):
        """Stores data to a CSV file."""
        try:
            source = data.get("source", "unknown")
            data_type = data.get("data_type", "general")

            # Create directory if it doesn't exist
            directory = os.path.join(self.data_dir, source)
            os.makedirs(directory, exist_ok=True)

            # Create filename based on data type
            filename = f"{data_type}.csv"
            filepath = os.path.join(directory, filename)

            # Convert data to DataFrame. Handle JSON and list data.
            df = pd.json_normalize(data)

            # Write data to CSV file.  Handle existing files.
            if not os.path.isfile(filepath):
                df.to_csv(filepath, index=False, header=True)  # Write header
            else:
                df.to_csv(filepath, index=False, mode='a', header=False) # Append, no header

            logger.debug(f"Data stored to {filepath}")

        except Exception as e:
            raise StorageError(f"Error storing data to CSV: {e}")



class PostgresDataStorage:
    """Handles PostgreSQL storage of collected data."""

    def __init__(self, config: Dict):
        """Initializes the data storage."""
        self.config = config
        self.pool = self._create_pool()  # Create connection pool on initialization

    def _create_pool(self):
        """Creates a connection pool."""
        try:
            return pool.ThreadedConnectionPool(
                minconn=self.config.get("min_connections", 1),
                maxconn=self.config.get("max_connections", 10),
                user=self.config["user"],
                password=self.config["password"],
                host=self.config["host"],
                port=self.config.get("port", 5432),  # Default PostgreSQL port
                database=self.config["database"]
            )
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            raise  # Re-raise the exception to halt execution if pool creation fails

    def _get_connection(self):
        """Gets a connection from the pool."""
        return self.pool.getconn()

    def _put_connection(self, conn):
        """Returns a connection to the pool."""
        self.pool.putconn(conn)


    def _create_table(self, conn, source: str, data_type: str, schema: Dict[str, str]):
        """Creates table if it doesn't exist."""

        cursor = conn.cursor()
        try:
            table_name = f"{source}_{data_type}"

            # Construct the CREATE TABLE statement using psycopg2.sql
            columns = [f"{col} {col_type}" for col, col_type in schema.items()]
            create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.SQL, columns))  # Correctly format columns
            )

            cursor.execute(create_table_query)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists.")

        finally:
            cursor.close()  # Always close the cursor


    def _determine_table_schema(self, data: Dict) -> Dict[str, str]:
        """Determines the table schema based on the data."""
        schema = {}
        for key, value in data.items():
            if key == "raw_data":  # Handle raw_data as JSONB
                schema[key] = "JSONB"
            elif isinstance(value, str):
                schema[key] = "TEXT"  # Use TEXT for strings
            elif isinstance(value, int):
                schema[key] = "BIGINT"  # Use BIGINT for integers
            elif isinstance(value, float):
                schema[key] = "DOUBLE PRECISION"  # Use DOUBLE PRECISION for floats
            elif isinstance(value, list):
                if all(isinstance(item, str) for item in value):
                  schema[key] = "TEXT[]"
                elif all(isinstance(item, (int, float)) for item in value):
                    schema[key] = "DOUBLE PRECISION[]"  # Array of floats/ints
                else:
                  schema[key] = "JSONB"
            elif isinstance(value, dict):
                schema[key] = "JSONB"  # Use JSONB for nested data
            elif value is None:
                # Skip columns with None values. They will be omitted in the INSERT.
                continue
            else:
                logger.warning(f"Unhandled data type for {key}: {type(value)}")
                schema[key] = "TEXT"  # Default to TEXT for safety

        return schema

    def store_data(self, data: Dict):
        """Stores data in the database."""
        conn = self._get_connection()
        cursor = None  # Initialize cursor outside the try block
        try:
            source = data.get("source", "unknown")
            data_type = data.get("data_type", "general")
            table_name = f"{source}_{data_type}"

            # Determine schema and create table if necessary
            schema = self._determine_table_schema(data)
            self._create_table(conn, source, data_type, schema)

            cursor = conn.cursor()  # Get cursor inside the try block

            # Prepare data, handling None/null and JSON.
            data_for_insert = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in data.items() if v is not None}
            columns = data_for_insert.keys()
            values = [data_for_insert[col] for col in columns]

            # Construct and execute INSERT statement.
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
            cursor.execute(insert_query, values)
            conn.commit()
            logger.debug(f"Data stored to {table_name}")

        except Exception as e:
            logger.error(f"Error storing data to database: {e}")
            conn.rollback()  # Rollback transaction on error
            raise StorageError(f"Database error: {e}") from e  # Re-raise as StorageError
        finally:
            if cursor:
                cursor.close() # Close the cursor if it was opened
            self._put_connection(conn) # Return connection to pool