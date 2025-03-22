import logging
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

from src.cleaning.base_etl import BaseETLPipeline

logger = logging.getLogger(__name__)


class ParkingMetersETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        """
        Data cleansing and transformation decisions:
        1. Drop irrelevant or mostly empty columns
        2. Handle missing values (fill or impute as appropriate)
        3. Correct data type inconsistencies
        4. Address outliers
        5. Create derived fields and enhanced features
        6. Encode and normalize categorical variables
        """
        # Columns with no coherent or relevant information for further analysis
        columns_to_drop = [
            "PARKING_SPACE_ID",
            "MS_PAY_STATION_ID",
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

        # 2. DATA TYPE CONVERSION AND VALIDATION

        # Numeric columns that need conversion and validation
        numeric_cols = {
            'OBJECTID': int,
            'MS_SPACE_NUM': int,
            'PM_DISTRICT_ID': float,
            'BLOCKFACE_ID': int,
            'STREET_ID': float,
            'STREET_NUM': float,
            'STREET_SEG_CTRLN_ID': float,
            'LONGITUDE': float,
            'LATITUDE': float
        }

        for col, dtype in numeric_cols.items():
            if col in records_df.columns:
                # Create a copy of the original column for validation
                original_col = records_df[col].copy()

                # Convert to numeric with errors coerced to NaN
                records_df[col] = pd.to_numeric(records_df[col], errors='coerce')

                # Check which rows failed conversion and log errors
                for idx, (orig_val, converted_val) in enumerate(zip(original_col, records_df[col])):
                    if pd.notna(orig_val) and pd.isna(converted_val):
                        record_id = records_df.iloc[idx].get('OBJECTID', idx)
                        self.log_error(col, f"Invalid {dtype.__name__} value: '{orig_val}'", record_id, 1001)

        # 3. HANDLE MISSING VALUES
        # A. Essential identifier and reference columns - no imputation
        id_cols = ['OBJECTID', 'POST_ID', 'PARKING_SPACE_ID']
        for col in id_cols:
            if col in records_df.columns:
                missing_ids = records_df[pd.isna(records_df[col])].index
                for idx in missing_ids:
                    self.log_error(col, f"Missing required identifier", idx, 1002)

        # B. Categorical fields - fill with 'Unknown' or most common value
        categorical_cols = [
            'ON_OFFSTREET_TYPE', 'JURISDICTION', 'ACTIVE_METER_FLAG',
            'REASON_CODE', 'METER_TYPE', 'METER_VENDOR', 'METER_MODEL', 'CAP_COLOR'
        ]

        for col in categorical_cols:
            if col in records_df.columns:
                # If less than 15% are missing, fill with mode
                missing_pct = records_df[col].isna().mean()
                if missing_pct < 0.15 and records_df[col].mode().size > 0:
                    mode_value = records_df[col].mode()[0]
                    records_df[col].fillna(mode_value, inplace=True)
                    logger.info(f"Filled {col} missing values ({missing_pct:.1%}) with mode: {mode_value}")
                else:
                    # Otherwise fill with 'Unknown'
                    records_df[col].fillna('Unknown', inplace=True)
                    logger.info(f"Filled {col} missing values ({missing_pct:.1%}) with 'Unknown'")

        # C. Numeric columns - different strategies based on column
        # Missing MS_SPACE_NUM is likely 0 for single-space meters
        if 'MS_SPACE_NUM' in records_df.columns:
            records_df['MS_SPACE_NUM'].fillna(0, inplace=True)

        # District identifiers - fill with -1 to indicate missing
        district_cols = ['PM_DISTRICT_ID', 'supervisor_district']
        for col in district_cols:
            if col in records_df.columns:
                records_df[col].fillna(-1, inplace=True)

        # 4. HANDLE OUTLIERS
        # Geographical coordinates - identify invalid coordinates
        if 'LONGITUDE' in records_df.columns and 'LATITUDE' in records_df.columns:
            # San Francisco bounding box (approximate by +-.3)
            sf_lon_min, sf_lon_max = -123.0, -122.0
            sf_lat_min, sf_lat_max = 37.6, 38.0

            # Find coordinates outside of San Francisco
            invalid_coords = (
                    (records_df['LONGITUDE'] < sf_lon_min) |
                    (records_df['LONGITUDE'] > sf_lon_max) |
                    (records_df['LATITUDE'] < sf_lat_min) |
                    (records_df['LATITUDE'] > sf_lat_max) |
                    (records_df['LONGITUDE'].isna()) |
                    (records_df['LATITUDE'].isna())
            )

            # Log errors for invalid coordinates
            for idx in records_df[invalid_coords].index:
                lon = records_df.loc[idx, 'LONGITUDE']
                lat = records_df.loc[idx, 'LATITUDE']
                record_id = records_df.loc[idx, 'OBJECTID'] if 'OBJECTID' in records_df.columns else idx
                self.log_error('COORDINATES', f"Invalid coordinates: ({lon}, {lat})", record_id, 1003)

            # For analysis, we'll set invalid coordinates to NaN
            records_df.loc[invalid_coords, ['LONGITUDE', 'LATITUDE']] = np.nan

        # 5. FEATURE ENGINEERING AND TRANSFORMATIONS
        # A. Decode categorical variables into meaningful labels
        # ON_OFFSTREET_TYPE decoder
        on_off_street_map = {
            "ON": "ON_STREET",
            "OFF": "OFF_STREET"
        }
        if 'ON_OFFSTREET_TYPE' in records_df.columns:
            records_df["ON_OFFSTREET_TYPE_DESC"] = records_df["ON_OFFSTREET_TYPE"].map(on_off_street_map).fillna(
                "Unknown")

        # ACTIVE_METER_FLAG decoder
        active_meter_flag_map = {
            "M": "Active",
            "T": "Temporarily Inactive",
            "P": "Pay-by-license plate",
            "L": "Legislated for future install",
            "U": "Unmetered"
        }
        if 'ACTIVE_METER_FLAG' in records_df.columns:
            records_df["METER_STATUS"] = records_df["ACTIVE_METER_FLAG"].map(active_meter_flag_map).fillna("Unknown")

        # METER_TYPE decoder
        meter_type_map = {
            "SS": "Single-space",
            "MS": "Multi-space"
        }
        if 'METER_TYPE' in records_df.columns:
            records_df["METER_TYPE_DESC"] = records_df["METER_TYPE"].map(meter_type_map).fillna("Unknown")

        # B. Create derived geographical features
        if 'LONGITUDE' in records_df.columns and 'LATITUDE' in records_df.columns:
            # Create GeoPoint field for valid coordinates
            valid_coords = records_df['LONGITUDE'].notna() & records_df['LATITUDE'].notna()
            records_df.loc[valid_coords, 'GEO_POINT'] = records_df.loc[valid_coords].apply(
                lambda row: f"POINT({row['LONGITUDE']} {row['LATITUDE']})", axis=1
            )

            # Create grid-based location feature (for spatial clustering)
            if valid_coords.any():
                # Round to 3 decimal places (roughly 100m precision)
                records_df.loc[valid_coords, 'LOCATION_GRID'] = records_df.loc[valid_coords].apply(
                    lambda row: f"{round(row['LONGITUDE'], 3)}_{round(row['LATITUDE'], 3)}",
                    axis=1
                )

        # D. Create flags for operational status
        if 'ACTIVE_METER_FLAG' in records_df.columns:
            records_df['IS_ACTIVE'] = records_df['ACTIVE_METER_FLAG'] == 'M'
            records_df['IS_TEMPORARY'] = records_df['ACTIVE_METER_FLAG'] == 'T'

        # E. Create features from multiple columns
        # Assign a zone based on district and neighborhood when available
        zone_cols = ['PM_DISTRICT_ID', 'analysis_neighborhood', 'supervisor_district']
        available_cols = [col for col in zone_cols if col in records_df.columns]

        if available_cols:
            primary_zone_col = available_cols[0]
            records_df['PARKING_ZONE'] = records_df[primary_zone_col].astype(str)
            if len(available_cols) > 1:
                for col in available_cols[1:]:
                    records_df['PARKING_ZONE'] = records_df['PARKING_ZONE'] + '_' + records_df[col].astype(str)

        # 6. DATA VALIDATION - Identify and log inconsistencies
        # Validate meters with location but no street information
        if all(col in records_df.columns for col in ['LONGITUDE', 'LATITUDE', 'STREET_NAME']):
            has_coords = records_df['LONGITUDE'].notna() & records_df['LATITUDE'].notna()
            no_street = records_df['STREET_NAME'].isna()
            inconsistent_records = records_df[has_coords & no_street]

            for idx in inconsistent_records.index:
                record_id = inconsistent_records.loc[idx, 'OBJECTID'] if 'OBJECTID' in records_df.columns else idx
                self.log_error('DATA_CONSISTENCY', "Has coordinates but missing street information", record_id, 2001)

        # Log completion of transform stage
        logger.info(f"Transform completed: {len(records_df)} records processed with {len(records_df.columns)} columns")

        return records_df


if __name__ == "__main__":
    etl_job = ParkingMetersETL(
        file_path=Path("../resources/raw/ParkingMeters.csv"),
        file_category="SanFrancisco",
    )
    etl_job.run()
