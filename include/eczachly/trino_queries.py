import trino
import logging

logger = logging.getLogger(__name__)
# The way we're doing the DQ check here is
def run_trino_query_dq_check(query):
    results = execute_trino_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True

def run_trino_dq(query: str) -> bool:
        """
        Executes a Trino query to check for duplicates.

        Args:
            query (str): The SQL query to identify duplicates.

        Returns:
            bool: True if duplicates are found, False otherwise.

        Raises:
            RuntimeError: If the query execution fails.
        """
        try:
            results = execute_trino_query(query)
            duplicate_count = len(results)
            logger.info(f"Duplicate count: {duplicate_count}")
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise RuntimeError(f"Failed to execute query: {e}") from e

        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate(s). Triggering cleaning process.")
            return True  # Indicates that duplicates were found
        else:
            logger.info("No duplicates found. Proceeding to load data to production.")
            return False  # Indicates that no duplicates were found

def execute_trino_query(query):
    conn = trino.dbapi.connect(
        host='',
        port=,
        user='',
        http_scheme='',
        catalog='',
        auth=trino.auth.BasicAuthentication(''),
    )
    print(query)
    cursor = conn.cursor()
    print("Executing query for the first time...")
    cursor.execute(query)
    return cursor.fetchall()
