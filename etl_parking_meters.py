from pathlib import Path

from pandas import DataFrame

from base_etl import BaseETLPipeline

class ParkingMetersETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        # Data Cleansing
        print(records_df.columns)

        # Columns with no coherent or relevant information for further analysis. Most of the columns are blank
        columns_to_drop = [
            "SENSOR_FLAG",
            "OSP_ID",
            "SMART_METER_FLAG",
            "PCO_BEAT",
            "OLD_RATE_AREA",
            "PARITY_DIGIT_POSITION",
            "ORIENTATION",
            "LEGISLATION_REF",
            "LEGISLATION_DT",
            "COMMENTS",
            "NFC_KEY",
            "SPT_CODE",
            "data_as_of",
            "data_loaded_at",
        ]
        records_df.drop(columns=columns_to_drop, inplace=True)

        # Data Transformation
        return records_df


if __name__ == "__main__":
    etl_job = ParkingMetersETL(
        file_path=Path("resources/raw/ParkingMeters.csv"),
        file_category="SanFrancisco",
    )
    etl_job.run()
