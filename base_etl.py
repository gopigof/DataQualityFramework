import logging
import os
import pprint
from datetime import datetime
from pathlib import Path

import pandas
from pandas import DataFrame

from db_utils import get_or_create_file_category_id, insert_file_name_record, insert_pipeline_observability_record

logging.basicConfig(
     level=logging.INFO,
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
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

        # DB Identifiers
        self.file_category_id = None
        self.file_id = None
        self.processing_file_id = None

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
        destination_file_path = self.file_path.parent.parent/ DATASET_FOLDER_CLEAN / f"processed_{self.file_path.stem}{self.file_path.suffix}"
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

    def log_metadata(self):
        logger.info(f"ETL Metadata Summary for {self.file_path}: \n{pprint.pformat(self.metadata, indent=4)}")

        # Get File_Category_Id FW_File_Category
        self.file_category_id = get_or_create_file_category_id(self.file_category)
        logger.info(f"{self.file_category_id=}")

        # Insert into FW_File_Name
        self.file_id = insert_file_name_record(file_name=self.file_path, file_category_id=self.file_category_id)

        # Insert into FW_Pipeline_Observability
        self.processing_file_id = insert_pipeline_observability_record(self.file_id, self.metadata)

        # Fetch the FW_Error_Message_Reference table into memory. Insert new errors as iteration proceeds
        self.errors = {}

        # Iterate over errors and insert FW_File_Record_Error for each row
        # Iterate over each FW_File_Record_Error and insert FW_Column_Error
