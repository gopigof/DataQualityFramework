import logging
import os
from datetime import datetime

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

logging.basicConfig(filename='data_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USERNAME = os.getenv("SQL_USERNAME")
PASSWORD = os.getenv("SQL_PASSWORD")
DRIVER = os.getenv("SQL_DRIVER")

conn_str = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
engine = create_engine(conn_str)
conn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}')
cursor = conn.cursor()

def insert_file_category():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM FW_File_Category WHERE File_Category_Name = ?)
        INSERT INTO FW_File_Category (File_Category_Name, Schema_Text) OUTPUT INSERTED.File_Category_Id VALUES (?, ?)
    """, ("CSV", "CSV", "Normal"))
    file_category_id = cursor.fetchone()[0]
    logging.info(f"Inserted File Category: 'CSV' with ID {file_category_id}")
    return file_category_id

def insert_file_name(file_category_id, file_name):
    cursor.execute("""
        INSERT INTO FW_File_Name (File_Category_Id, File_Name) OUTPUT INSERTED.File_Id VALUES (?, ?)
    """, (file_category_id, file_name))
    file_id = cursor.fetchone()[0]
    logging.info(f"Inserted file '{file_name}' into FW_File_Name with File_Id {file_id}")
    return file_id

def insert_pipeline_observability(file_id, num_records, file_size):
    processing_start = datetime.now()
    cursor.execute("""
        INSERT INTO FW_Pipeline_Observability (
            File_Id, Time_Of_Arrival, Process_StartTime, Input_File_Size, 
            Initial_Count_Of_Records
        ) OUTPUT INSERTED.Processing_File_Id
        VALUES (?, ?, ?, ?, ?)
    """, (file_id, datetime.now(), processing_start, file_size, num_records))
    processing_file_id = cursor.fetchone()[0]
    logging.info(f"Inserted into FW_Pipeline_Observability with Processing_File_Id {processing_file_id}")
    return processing_file_id

def process_records(df, processing_file_id):
    error_records = []
    distinct_errors = set()
    processed_records = 0
    error_count = 0

    for index, row in df.iterrows():
        record_text = row.to_json()
        error_found = False
        record_id = None

        try:
            # Insert into FW_File_Record_Error
            cursor.execute("""
                INSERT INTO FW_File_Record_Error (Processing_File_Id, Record_Text)
                OUTPUT INSERTED.Record_ID
                VALUES (?, ?)
            """, (processing_file_id, record_text))
            record_id = cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Failed to insert record at index {index}: {str(e)}")
            continue  # Skip this row entirely

        # Validate data types (assumption: first column should be integer)
        for col in df.columns:
            if df[col].dtype == 'int64' and not str(row[col]).isdigit():
                error_code = "INVALID_TYPE"
                error_message = f"Expected integer but found {row[col]}"
                error_found = True
                error_count += 1
                distinct_errors.add(error_code)

                # Insert into FW_Error_Message_Reference
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM FW_Error_Message_Reference WHERE Error_Message = ?)
                    INSERT INTO FW_Error_Message_Reference (Error_Message) OUTPUT INSERTED.Error_Id VALUES (?)
                """, (error_message, error_message))
                error_id = cursor.fetchone()[0]

                # Insert into FW_Column_Error
                cursor.execute("""
                    INSERT INTO FW_Column_Error (Record_ID, Error_Id, Column_Name, Error_Code)
                    VALUES (?, ?, ?, ?)
                """, (record_id, error_id, col, error_code))

                error_records.append({"Index": index, "Column": col, "Error_Code": error_code, "Error_Message": error_message})

        if not error_found:
            processed_records += 1

    return processed_records, error_count, distinct_errors, error_records

def update_pipeline_observability(processing_file_id, processed_records, error_count, distinct_errors):
    cursor.execute("""
        UPDATE FW_Pipeline_Observability 
        SET Process_End_Time = ?, Count_Of_Processed_Records = ?, Count_Of_Error_Records = ?, 
            Count_Of_Distinct_Errors = ?
        WHERE Processing_File_Id = ?
    """, (datetime.now(), processed_records, error_count, len(distinct_errors), processing_file_id))
    logging.info(f"Processing completed. Processed: {processed_records}, Errors: {error_count}")

def save_error_logs(error_records):
    output_dir = "resources"
    os.makedirs(output_dir, exist_ok=True)
    error_df = pd.DataFrame(error_records)
    error_output_path = os.path.join(output_dir, "error_log.csv")
    error_df.to_csv(error_output_path, index=False)
    logging.info(f"Error report saved at {error_output_path}")

def main():
    csv_file = 'your_data.csv'  # Update with actual file path
    df = pd.read_csv(csv_file)

    file_category_id = insert_file_category()
    file_id = insert_file_name(file_category_id, os.path.basename(csv_file))

    processing_file_id = insert_pipeline_observability(file_id, len(df), os.path.getsize(csv_file))
    processed_records, error_count, distinct_errors, error_records = process_records(df, processing_file_id)
    update_pipeline_observability(processing_file_id, processed_records, error_count, distinct_errors)

    if error_records:
        save_error_logs(error_records)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data successfully loaded and logged into SQL Server.")

if __name__ == "__main__":
    main()
