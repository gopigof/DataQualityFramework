import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

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
    return f"DRIVER={{{db_driver}}};SERVER={db_server};PORT=1433;DATABASE={db};UID={db_username};PWD={db_password}"


@contextmanager
def get_cursor():
    # logger.info(f"{_get_connection_string()=}")
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
        cursor.execute("SELECT File_Category_Id FROM FW_File_Category WHERE File_Category_Name = ?", file_category_name)
        return cursor.fetchall()


def get_or_create_file_category_id(file_category_name: str) -> int:
    category_ids = get_file_category_id(file_category_name=file_category_name)
    if len(category_ids) > 0:
        return category_ids[0][0]
    else:
        with get_cursor() as cursor:
            cursor.execute("INSERT INTO FW_File_Category VALUES (?)", file_category_name)
        return get_or_create_file_category_id(file_category_name)


def insert_file_name_record(file_name: Path, file_category_id: int) -> int:
    with get_cursor() as cursor:
        cursor.execute("INSERT INTO FW_File_Name VALUES (?, ?)", file_category_id, file_name.name)
    with get_cursor() as cursor:
        cursor.execute("SELECT MAX(File_Id) FROM FW_File_Name")
        results = cursor.fetchone()
    return results[0]


def insert_pipeline_observability_record(file_id: int, metadata: dict) -> int:
    with get_cursor() as cursor:
        cursor.execute("INSERT INTO FW_Pipeline_Observability VALUES (?,?,?,?,?,?,?,?,?)", file_id,
                       metadata["time_of_arrival"], metadata["process_start_time"], metadata["process_end_time"],
                       metadata["input_file_size"], metadata["initial_count_of_records"],
                       metadata["count_of_processed_records"],
                       metadata["count_of_error_records"], metadata["count_of_distinct_records"])
    with get_cursor() as cursor:
        cursor.execute("SELECT MAX(Processing_File_Id) FROM FW_Pipeline_Observability")
        results = cursor.fetchone()
    return results[0]


def get_existing_errors():
    with get_cursor() as cursor:
        cursor.execute("SELECT * FROM FW_Error_Message_Reference")
        results = cursor.fetchall()
    return {result[0]: result[1] for result in results}


def get_error_message_reference(error_message: str) -> tuple[str] | None:
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT Error_Id FROM FW_Error_Message_Reference WHERE Error_Message = ?", error_message[:255]
        )
        return cursor.fetchone()


def get_or_create_error_record(error_message: str) -> tuple[str] | None:
    error_obj = get_error_message_reference(error_message=error_message)
    if error_obj:
        return error_obj
    with get_cursor() as cursor:
        cursor.execute(
            "INSERT INTO FW_Error_Message_Reference VALUES (?)", error_message[:255],
        )
    return get_error_message_reference(error_message=error_message)


def insert_file_record_error(processing_file_id: int, record_text: str) -> int:
    with get_cursor() as cursor:
        cursor.execute("INSERT INTO FW_File_Record_Error (Processing_File_Id) VALUES (?)", processing_file_id)
    with get_cursor() as cursor:
        cursor.execute("SELECT MAX(Record_ID) FROM FW_File_Record_Error")
        results = cursor.fetchone()
    return results[0]


def insert_column_error(error_id: int, column_name: str, error_code: int, record_id: int) -> None:
    with get_cursor() as cursor:
        cursor.execute("INSERT INTO FW_Column_Error (Error_Id, Column_Name, Error_Code, Record_ID) VALUES (?, ?, ?, ?)",
                       error_id, column_name, error_code, record_id)


def get_current_processing_file_id() -> int:
    with get_cursor() as cursor:
        cursor.execute("SELECT COALESCE(MAX(File_Id), 1) AS Current_ID FROM FW_Pipeline_Observability")
        return cursor.fetchone()[0]
