"""Module for interacting with a PostgreSQL database.

This module provides functions for:

- Creating connection strings to a PostgreSQL database.
- Inserting a pandas DataFrame into a PostgreSQL table.
- Executing SQL queries against a PostgreSQL database."""

from typing import Dict

import pandas as pd
from loguru import logger
from psycopg2 import OperationalError, ProgrammingError, connect
from sqlalchemy import create_engine


def _create_postgres_connection_string(conn_details: Dict) -> str:
    try:
        conn_string = (
            f"postgresql://{conn_details['username']}:{conn_details['password']}@"
            f"{conn_details['host']}:{conn_details['port']}/{conn_details['database']}"
        )
        logger.info("[✅] Generated PostgreSQL connection string.")
        return conn_string
    except (TypeError, ValueError) as e:
        logger.error(f"[❌] Error creating connection string: {e}")
        raise e


def write_dataframe_to_postgres(
    df: pd.DataFrame, schema: str, table: str, conn_details: Dict
):
    db_url = _create_postgres_connection_string(conn_details=conn_details)
    try:
        engine = create_engine(db_url)
        df.to_sql(
            schema=schema, name=table, con=engine, if_exists="append", index=False
        )
        logger.info(
            f"[✅] DataFrame successfully written to '{schema}.{table}' in PostgreSQL."
        )
    except (ValueError, OperationalError) as e:
        logger.error(f"[❌] Error writing DataFrame to PostgreSQL: {e}")
        raise e


def run_postgresql_query(query: str, conn_details: Dict) -> pd.DataFrame:
    db_url = _create_postgres_connection_string(conn_details=conn_details)
    result = pd.DataFrame()
    try:
        conn = connect(db_url)
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description is not None:
            columns = [col[0] for col in cursor.description]
            result_list = cursor.fetchall()
            result = pd.DataFrame(result_list, columns=columns)
        conn.commit()
        cursor.close()
        conn.close()
        if result.shape[0] != 0:
            logger.info(
                f"[✅] The data was successfully fetched with a shape of {result.shape}."
            )
        else:
            logger.info("[✅] The query was executed successfully.")
        return result
    except (ProgrammingError, OperationalError) as e:
        logger.error(f"[❌] Error executing query: {e}")
        raise e
