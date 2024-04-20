"""Module for processing calendar data in chunks and storing it in a PostgreSQL database.

This module orchestrates the process of:

1. Retrieving calendar data for a specified date range in chunks.
2. Writing each chunk of calendar data to a temporary source table in a PostgreSQL database.
3. Merging the data from the source table into the target table using a user-defined SQL query.
4. Truncating the source table to prepare it for the next chunk.
5. Repeating steps 1-4 until the entire date range is processed."""

import os
from datetime import datetime, timedelta
from typing import Dict

from loguru import logger

from src import get_calendar_with_jalali_and_holidays
from src.utils import (
    create_source_and_target_tables,
    create_target_table_index,
    log_configure,
    merge_source_into_target,
    read_yaml_file,
    truncate_source_table,
    write_dataframe_to_postgres,
)

CONN_DETAILS = {
    "username": os.environ["PG_USERNAME"],
    "password": os.environ["PG_PASSWORD"],
    "host": os.environ["PG_HOST"],
    "port": os.environ["PG_PORT"],
    "database": os.environ["PG_DATABASE"],
}
TABLES_DETAILS = {
    "source": {
        "schema": os.environ["SOURCE_SCHEMA_NAME"],
        "table": os.environ["SOURCE_TABLE_NAME"],
    },
    "target": {
        "schema": os.environ["TARGET_SCHEMA_NAME"],
        "table": os.environ["TARGET_TABLE_NAME"],
    },
}
DATE_RANGES = {
    "start_date": os.environ["START_DATE"],
    "end_date": os.environ["END_DATE"],
}
CONFIG = read_yaml_file(path="src/config/queries.yaml")


def process_data_in_chunks(
    date_ranges: Dict[str, str],
    chunk_size: int,
    config_details: Dict[str, str],
    conn_details: Dict[str, str],
    tables_details: Dict[str, Dict[str, str]],
):
    date_format = "%Y-%m-%d"
    start_date = date_ranges["start_date"]
    end_date = date_ranges["end_date"]
    current_date = start_date

    while current_date <= end_date:

        next_chunk_end_date = min(
            (
                datetime.strptime(current_date, date_format)
                + timedelta(days=chunk_size)
            ).strftime(date_format),
            end_date,
        )

        chunk_calendar_df = get_calendar_with_jalali_and_holidays(
            start_date=current_date, end_date=next_chunk_end_date
        )

        write_dataframe_to_postgres(
            df=chunk_calendar_df,
            schema=tables_details["source"]["schema"],
            table=tables_details["source"]["table"],
            conn_details=conn_details,
        )
        merge_source_into_target(
            query=config_details["merge_source_into_target"],
            conn_details=conn_details,
            tables_details=tables_details,
        )
        truncate_source_table(
            query=config_details["truncate_source_table"],
            conn_details=conn_details,
            tables_details=tables_details,
        )

        logger.info(
            f"[✅] Successfully added the date range ({current_date} -"
            f"{next_chunk_end_date}) to the database.",
        )

        current_date = (
            datetime.strptime(next_chunk_end_date, date_format) + timedelta(days=1)
        ).strftime(date_format)


@logger.catch
def main(
    date_ranges: Dict[str, str],
    config_details: Dict[str, str],
    conn_details: Dict[str, str],
    tables_details: Dict[str, Dict[str, str]],
    chunk_size: int = 30,
):
    create_source_and_target_tables(
        query=config_details["create_table"],
        conn_details=conn_details,
        tables_details=tables_details,
    )
    create_target_table_index(
        query=config_details["create_target_table_index"],
        conn_details=conn_details,
        tables_details=tables_details,
    )
    process_data_in_chunks(
        date_ranges=date_ranges,
        chunk_size=chunk_size,
        config_details=config_details,
        conn_details=conn_details,
        tables_details=tables_details,
    )
    logger.info(
        "[✅] Successfully added the whole date range to the database.",
    )


if __name__ == "__main__":

    log_configure()
    main(
        date_ranges=DATE_RANGES,
        config_details=CONFIG,
        conn_details=CONN_DETAILS,
        tables_details=TABLES_DETAILS,
    )
