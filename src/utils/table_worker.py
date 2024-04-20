"""Module for managing data operations in a PostgreSQL database.

This module provides functions to create, manipulate, and interact with tables
in a PostgreSQL database. It offers functionalities for:

- Creating source and target tables using a provided SQL query.
- Creating indexes on target tables.
- Merging data from a source table into a target table using a user-defined SQL query.
- Truncating (deleting all data) from a source table."""

from typing import Dict

from loguru import logger
from psycopg2 import Error

from .db_connector import run_postgresql_query


def create_source_and_target_tables(
    query: str, conn_details: Dict[str, str], tables_details: Dict[str, Dict[str, str]]
):

    for table_type, details in tables_details.items():
        schema, table = details.values()
        formatted_query = query.format(schema=schema, table=table)

        try:
            run_postgresql_query(formatted_query, conn_details)
            logger.info(f"[✅] Table '{schema}.{table}' created successfully.")
        except (Error, ConnectionError) as e:
            logger.error(f"[❌] Error creating {table_type} table: {e}")
            raise e


def create_target_table_index(
    query: str, conn_details: Dict[str, str], tables_details: Dict[str, Dict[str, str]]
):
    schema, table = tables_details["target"].values()
    formatted_query = query.format(
        target_schema=schema,
        target_table=table,
    )
    try:
        run_postgresql_query(formatted_query, conn_details)
        logger.info(f"[✅] Index of Table '{schema}.{table}' created successfully.")
    except (Error, ConnectionError) as e:
        logger.error(f"[❌] Error creating '{schema}.{table}' table index: {e}")
        raise e


def merge_source_into_target(
    query: str, conn_details: Dict[str, str], tables_details: Dict[str, Dict[str, str]]
):
    formatted_query = query.format(
        source_schema=tables_details["source"]["schema"],
        source_table=tables_details["source"]["table"],
        target_schema=tables_details["target"]["schema"],
        target_table=tables_details["target"]["table"],
    )
    try:
        run_postgresql_query(formatted_query, conn_details)
        logger.info("[✅] Source merged into target table successfully.")
    except (Error, ConnectionError) as e:
        logger.error(f"[❌] Error merging source into target table: {e}")
        raise e


def truncate_source_table(
    query: str, conn_details: Dict[str, str], tables_details: Dict[str, Dict[str, str]]
):
    schema, table = tables_details["source"].values()
    formatted_query = query.format(
        source_schema=schema,
        source_table=table,
    )
    try:
        run_postgresql_query(formatted_query, conn_details)
        logger.info(f"[✅] Table '{schema}.{table}' truncated successfully.")
    except (Error, ConnectionError) as e:
        logger.error(f"[❌] Error truncating '{schema}.{table}' table: {e}")
        raise e
