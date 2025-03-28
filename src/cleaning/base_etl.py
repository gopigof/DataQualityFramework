import logging
import os
import pprint
from datetime import datetime
from pathlib import Path

import pandas
from pandas import DataFrame

from src.cleaning.db_utils import get_or_create_file_category_id, insert_file_name_record, insert_pipeline_observability_record, \
    insert_file_record_error, insert_column_error, get_existing_errors, get_or_create_error_record, \
    get_current_processing_file_id

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Dataset folder names - /resources/
DATASET_FOLDER_RAW = "raw"
DATASET_FOLDER_CLEAN = "clean"


class BaseETLPipeline:
    def __init__(self, file_path, file_category):
        self.file_path = Path(file_path)
        self.file_category = file_category
        self.metadata: dict = {
            "time_of_arrival": None,
            "process_start_time": None,
            "process_end_time": None,
            "input_file_size": 0,
            "initial_count_of_records": 0,
            "count_of_processed_records": 0,
            "count_of_distinct_records": 0,  # TODO: Replace with distinct errors
            "count_of_error_records": 0
        }
        self.errors = None
        self.existing_errors: dict[int, str] = get_existing_errors()

        # DB Identifiers
        self.file_category_id = None
        self.file_id = None
        self.processing_file_id = get_current_processing_file_id()

    def extract(self) -> DataFrame:
        self.metadata["process_start_time"] = datetime.now()
        self.metadata["time_of_arrival"] = datetime.now()
        try:
            self.metadata["input_file_size"] = os.path.getsize(self.file_path)
            records_df = pandas.read_csv(self.file_path)
            self.metadata["initial_count_of_records"] = len(records_df.index)
            # self.metadata["count_of_distinct_records"] = int(records_df.duplicated().value_counts().loc[False])
            return records_df
        except Exception as e:
            logger.error(f"Error encountered creating dataframe from file: {self.file_path}")
            raise

    def transform(self, records_df: DataFrame):
        return records_df

    def load(self, records_df: DataFrame):
        (self.file_path.parent.parent / DATASET_FOLDER_CLEAN).mkdir(exist_ok=True)
        destination_file_path = self.file_path.parent.parent / DATASET_FOLDER_CLEAN / f"processed_{self.file_path.stem}{self.file_path.suffix}"
        records_df.to_csv(destination_file_path, index=False)
        self.metadata["count_of_processed_records"] = len(records_df.index)

    def run(self):
        try:
            records_df = self.extract()
            records_df = self.transform(records_df)
            self.load(records_df)
        finally:
            self.metadata["process_end_time"] = datetime.now()
            self.log_metadata()

    def log_error(self, column_name, error_message, record_id=None, error_code=None):
        """Log errors to database"""
        if self.errors is None:
            self.errors = {}

        # Track distinct errors for reporting
        error_key = f"{column_name}:{error_message}"
        if error_key not in self.errors:
            self.errors[error_key] = 0
        self.errors[error_key] += 1

        # Get or create error message reference
        error_id = get_or_create_error_record(error_message)[0]

        # If this is a record-specific error, track in FW_File_Record_Error and FW_Column_Error
        if self.processing_file_id is not None:
            # Create a string representation of the record
            record_text = f"Column: {column_name}, Error: {error_message[:100]}"

            # Insert into FW_File_Record_Error
            db_record_id = insert_file_record_error(self.processing_file_id, record_text)

            # Insert into FW_Column_Error
            insert_column_error(int(error_id), column_name, error_code or 0, db_record_id)

        # Update error counts
        self.metadata["count_of_error_records"] += 1
        self.metadata["count_of_distinct_records"] = len(self.errors)

        logger.warning(f"Error in column '{column_name}': {error_message} (Record ID: {record_id})")

    def log_metadata(self):
        """Log ETL metadata to database and console"""
        logger.info(f"ETL Metadata Summary for {self.file_path}: \n{pprint.pformat(self.metadata, indent=4)}")

        # Update count of distinct errors
        if self.errors:
            self.metadata["count_of_distinct_records"] = len(self.errors)

        # Get File_Category_Id from FW_File_Category
        self.file_category_id = get_or_create_file_category_id(self.file_category)
        logger.info(f"{self.file_category_id=}")

        # Insert into FW_File_Name
        self.file_id = insert_file_name_record(file_name=self.file_path, file_category_id=self.file_category_id)
        logger.info(f"File registered with ID: {self.file_id}")

        # Insert into FW_Pipeline_Observability
        self.processing_file_id = insert_pipeline_observability_record(self.file_id, self.metadata)
        logger.info(f"Processing record created with ID: {self.processing_file_id}")

        # If we have errors collected during processing but not yet saved to DB
        # (this can happen if errors are logged before processing_file_id is set)
        if self.errors and any(error_key for error_key in self.errors):
            logger.info(f"Logging {len(self.errors)} distinct error types to database")
            for error_key, count in self.errors.items():
                column_name, error_message = error_key.split(":", 1)
                # Just log the error type without associating with specific records
                error_id = get_or_create_error_record(error_message)[0]
                logger.debug(f"Logged error type: {error_key} (Count: {count}, Error ID: {error_id})")

        # Summary
        processing_time = (self.metadata["process_end_time"] - self.metadata["process_start_time"]).total_seconds()
        logger.info(f"ETL Process completed in {processing_time:.2f} seconds")
        logger.info(f"Processed {self.metadata['count_of_processed_records']} records")
        logger.info(
            f"Encountered {self.metadata['count_of_error_records']} errors of {self.metadata['count_of_distinct_records']} distinct types")
