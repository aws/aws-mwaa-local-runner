from typing import List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

class PostgresQueryHook(PostgresHook):
    """
    A custom hook for querying a PostgreSQL database using Airflow's PostgresHook.

    This hook extends the PostgresHook from the airflow.providers.postgres package,
    and provides a simplified API for executing SQL queries and fetching the results.

    Example usage:
        hook = PostgresQueryHook(postgres_conn_id='my_postgres_conn', schema='my_schema')
        results = hook.get_results('SELECT * FROM my_table WHERE my_column = %s', ('my_value',))
        for row in results:
            print(row)

    Args:
        postgres_conn_id: The Airflow connection ID for the PostgreSQL database.
        schema: The name of the schema to use in the queries.
    """
    def __init__(self, postgres_conn_id: str, schema: str):
        """
        Initializes a new instance of the PostgresQueryHook.

        Args:
            postgres_conn_id: The Airflow connection ID for the PostgreSQL database.
            schema: The name of the schema to use in the queries.
        """
        super().__init__(postgres_conn_id=postgres_conn_id)
        self.schema = schema
    
    def get_results(self, query: str, parameters: Tuple = None, max_retries: int = 3, retry_delay_secs: int = 20) -> List[Tuple]:
        """
        Executes a SQL query on the PostgreSQL database and returns the results as a list of tuples.

        This method automatically creates a new database connection, executes the query,
        fetches the results, and closes the connection.

        Args:
            query: The SQL query string to execute.
            parameters: Optional tuple of query parameters to substitute into the query.
            max_retries: The maximum number of times to retry the query in case of an error.
            retry_delay_secs: The delay between retries in seconds.

        Returns:
            A list of tuples representing the query results.
        """
        for i in range(max_retries + 1):
            try:
                with self.get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query, parameters)
                        return cursor.fetchall()
            except Exception as e:
                if i < max_retries:
                    self.log.warning(f"Query failed ({str(e)}). Retrying in {retry_delay_secs} seconds...")
                    time.sleep(retry_delay_secs)
                else:
                    self.log.error(f"Query failed after {max_retries} retries. Last error: {str(e)}")
                    raise