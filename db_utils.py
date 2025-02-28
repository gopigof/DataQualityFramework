import logging
import os
from contextlib import contextmanager
from functools import lru_cache

import pyodbc
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

@lru_cache
def _get_connection_string() -> str:
    db_username = os.getenv("SQL_USERNAME")
    db_password = os.getenv("SQL_PASSWORD")
    db = os.getenv("SQL_DATABASE")
    db_server = os.getenv("SQL_SERVER")
    db_driver = os.getenv("SQL_DRIVER")
    return f"Driver={{{db_driver}}};Server={db_server};Database={db};User Id={db_username};Password={db_password}"
    #return f"mssql+pyodbc://{db_username}:{db_password}@{db_server}/{db}?driver={db_driver}"


@contextmanager
def get_cursor():
    logger.info(f"{_get_connection_string()=}")
    with pyodbc.connect(_get_connection_string()) as connection:
        with connection.cursor() as cursor:
            try:
                yield cursor
                connection.commit()
            except Exception:
                connection.rollback()
                raise


def get_file_category_id(file_category_name: str) -> list[pyodbc.Row]:
    with get_cursor() as cursor:
        cursor.execute("SELECT File_Category_Id FROM FW_File_Category WHERE File_Category_Name = ?;", file_category_name)
        return cursor.fetchall()


def get_or_create_file_category_id(file_category_name: str) -> int:
    category_ids = get_file_category_id(file_category_name=file_category_name)
    if len(category_ids) > 0:
        return category_ids[0]
    else:
        with get_cursor() as cursor:
            cursor.execute("INSERT INTO FW_File_Category VALUES (?, '')", file_category_name)
        return get_or_create_file_category_id(file_category_name)
