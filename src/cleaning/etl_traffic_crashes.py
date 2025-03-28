import logging
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.cleaning.base_etl import BaseETLPipeline

logger = logging.getLogger(__name__)


class TrafficCrashesETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        # 1. REMOVING IRRELEVANT OR DUPLICATIVE COLUMNS
        columns_to_drop = [
            "cnn_intrsctn_fkey",
            "cnn_sgmt_fkey",
            "street_view",
            "data_as_of",
            "data_updated_at",
            "data_loaded_at",
            "Analysis Neighborhoods",
            "Neighborhoods",
            "SF Find Neighborhoods",
            "Current Police Districts",
            "Current Supervisor Districts",
            "vz_pcf_link"
        ]
        records_df.drop(columns=[col for col in columns_to_drop if col in records_df.columns], inplace=True)

        # 2. DATA TYPE CONVERSION AND VALIDATION
        # Date and time processing
        if 'collision_datetime' in records_df.columns:
            try:
                # Convert to datetime with error handling
                records_df['collision_datetime'] = pd.to_datetime(
                    records_df['collision_datetime'],
                    errors='coerce'
                )

                # Log records with invalid datetime
                invalid_datetime = records_df['collision_datetime'].isna()
                if 'collision_date' in records_df.columns and 'collision_time' in records_df.columns:
                    # Try to reconstruct from separate date and time fields
                    for idx in records_df[invalid_datetime].index:
                        date_str = records_df.loc[idx, 'collision_date']
                        time_str = records_df.loc[idx, 'collision_time']
                        try:
                            combined = f"{date_str} {time_str}"
                            records_df.loc[idx, 'collision_datetime'] = pd.to_datetime(combined)
                        except:
                            record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                            self.log_error('collision_datetime',
                                           f"Invalid datetime: date={date_str}, time={time_str}",
                                           record_id, 1001)
            except Exception as e:
                self.log_error('collision_datetime', f"Error processing datetime: {str(e)}")

        # Numeric columns that need conversion and validation
        numeric_cols = {
            'unique_id': int,
            'case_id_pkey': int,
            'tb_latitude': float,
            'tb_longitude': float,
            'distance': float,
            'number_killed': float,
            'number_injured': int,
            'party_at_fault': float,
            'supervisor_district': float,
            'accident_year': int
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
                        record_id = records_df.iloc[idx].get('unique_id', idx)
                        self.log_error(col, f"Invalid {dtype.__name__} value: '{orig_val}'", record_id, 1002)

        # 3. HANDLE MISSING VALUES
        # A. Essential identifier columns - no imputation
        id_cols = ['unique_id', 'case_id_pkey']
        for col in id_cols:
            if col in records_df.columns:
                missing_ids = records_df[pd.isna(records_df[col])].index
                for idx in missing_ids:
                    self.log_error(col, f"Missing required identifier", idx, 1003)

        # B. Date/time fields - critical for analysis
        datetime_cols = ['collision_datetime']
        for col in datetime_cols:
            if col in records_df.columns:
                missing_datetime = records_df[pd.isna(records_df[col])].index
                for idx in missing_datetime:
                    record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                    self.log_error(col, f"Missing datetime information", record_id, 1004)

        # C. Location fields - important for geospatial analysis
        if 'tb_latitude' in records_df.columns and 'tb_longitude' in records_df.columns:
            missing_coords = records_df[
                (pd.isna(records_df['tb_latitude'])) | (pd.isna(records_df['tb_longitude']))
                ].index
            for idx in missing_coords:
                record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                self.log_error('COORDINATES', f"Missing coordinates", record_id, 1005)

        # D. Categorical fields - fill with 'Unknown' for analysis
        categorical_cols = [
            'geocode_source', 'geocode_location', 'month', 'day_of_week', 'time_cat',
            'juris', 'weather_1', 'weather_2', 'collision_severity', 'type_of_collision',
            'mviw', 'ped_action', 'road_surface', 'road_cond_1', 'road_cond_2',
            'lighting', 'control_device', 'intersection', 'vz_pcf_code', 'vz_pcf_group',
            'vz_pcf_description', 'dph_col_grp', 'dph_col_grp_description',
            'party1_type', 'party1_dir_of_travel', 'party1_move_pre_acc',
            'party2_type', 'party2_dir_of_travel', 'party2_move_pre_acc',
            'police_district', 'analysis_neighborhood'
        ]

        for col in categorical_cols:
            if col in records_df.columns:
                records_df[col].fillna('Unknown', inplace=True)

        # E. Numeric count fields - fill with 0 where appropriate
        if 'number_killed' in records_df.columns:
            records_df['number_killed'].fillna(0, inplace=True)
        if 'number_injured' in records_df.columns:
            records_df['number_injured'].fillna(0, inplace=True)

        # 4. HANDLE OUTLIERS AND VALIDATE DATA
        # Geographical coordinates - identify invalid coordinates
        if 'tb_latitude' in records_df.columns and 'tb_longitude' in records_df.columns:
            # San Francisco bounding box (approximate)
            sf_lon_min, sf_lon_max = -123.0, -122.0
            sf_lat_min, sf_lat_max = 37.6, 38.0

            # Find coordinates outside of San Francisco
            invalid_coords = records_df[
                (records_df['tb_longitude'] < sf_lon_min) |
                (records_df['tb_longitude'] > sf_lon_max) |
                (records_df['tb_latitude'] < sf_lat_min) |
                (records_df['tb_latitude'] > sf_lat_max)
                ].index

            # Log errors for invalid coordinates
            for idx in invalid_coords:
                lon = records_df.loc[idx, 'tb_longitude']
                lat = records_df.loc[idx, 'tb_latitude']
                record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                self.log_error('COORDINATES', f"Invalid coordinates: ({lon}, {lat})", record_id, 1006)

        # Validate accident year is consistent with collision_datetime
        if 'collision_datetime' in records_df.columns and 'accident_year' in records_df.columns:
            for idx, row in records_df.iterrows():
                if pd.notna(row['collision_datetime']) and pd.notna(row['accident_year']):
                    datetime_year = row['collision_datetime'].year
                    if datetime_year != row['accident_year']:
                        record_id = row['unique_id'] if 'unique_id' in records_df.columns else idx
                        self.log_error('accident_year',
                                       f"Year mismatch: datetime={datetime_year}, accident_year={row['accident_year']}",
                                       record_id, 1007)
                        # Correct the mismatch by using the datetime value
                        records_df.loc[idx, 'accident_year'] = datetime_year

        # 5. FEATURE ENGINEERING AND TRANSFORMATIONS
        # A. Create derived datetime fields
        if 'collision_datetime' in records_df.columns:
            # Extract components if not already available
            if 'month' not in records_df.columns or records_df['month'].isna().any():
                records_df['month'] = records_df['collision_datetime'].dt.strftime('%B')

            if 'day_of_week' not in records_df.columns or records_df['day_of_week'].isna().any():
                records_df['day_of_week'] = records_df['collision_datetime'].dt.strftime('%A')

            # Create hour of day
            records_df['hour_of_day'] = records_df['collision_datetime'].dt.hour

            # Create weekend flag
            records_df['is_weekend'] = records_df['collision_datetime'].dt.dayofweek >= 5

            # Create time of day category
            def categorize_time(hour):
                if pd.isna(hour):
                    return 'Unknown'
                elif 0 <= hour < 6:
                    return 'Night (12AM-6AM)'
                elif 6 <= hour < 12:
                    return 'Morning (6AM-12PM)'
                elif 12 <= hour < 18:
                    return 'Afternoon (12PM-6PM)'
                else:
                    return 'Evening (6PM-12AM)'

            records_df['time_of_day'] = records_df['hour_of_day'].apply(categorize_time)

            # Create season
            def get_season(month):
                if pd.isna(month):
                    return 'Unknown'
                month_num = pd.to_datetime(month, format='%B').month
                if 3 <= month_num <= 5:
                    return 'Spring'
                elif 6 <= month_num <= 8:
                    return 'Summer'
                elif 9 <= month_num <= 11:
                    return 'Fall'
                else:
                    return 'Winter'

            records_df['season'] = records_df['month'].apply(get_season)

        # B. Create severity features
        if 'number_killed' in records_df.columns and 'number_injured' in records_df.columns:
            # Create total casualties
            records_df['total_casualties'] = records_df['number_killed'] + records_df['number_injured']

            # Create fatality flag
            records_df['has_fatality'] = records_df['number_killed'] > 0

            # Create injury flag
            records_df['has_injury'] = records_df['number_injured'] > 0

            # Create severity score (weighted sum where fatalities count more)
            records_df['severity_score'] = records_df['number_killed'] * 5 + records_df['number_injured']

        # C. Create participant type features
        if 'party1_type' in records_df.columns and 'party2_type' in records_df.columns:
            # Create flags for specific participant types
            for party_type in ['Pedestrian', 'Bicycle', 'Motorcycle', 'Vehicle']:
                records_df[f'involves_{party_type.lower()}'] = (
                        records_df['party1_type'].str.contains(party_type, case=False, na=False) |
                        records_df['party2_type'].str.contains(party_type, case=False, na=False)
                )

        # D. Create geospatial feature for valid coordinates
        if 'tb_longitude' in records_df.columns and 'tb_latitude' in records_df.columns:
            valid_coords = records_df['tb_longitude'].notna() & records_df['tb_latitude'].notna()
            records_df.loc[valid_coords, 'geo_point'] = records_df.loc[valid_coords].apply(
                lambda row: f"POINT({row['tb_longitude']} {row['tb_latitude']})", axis=1
            )

            # Create location grid for clustering
            records_df.loc[valid_coords, 'location_grid'] = records_df.loc[valid_coords].apply(
                lambda row: f"{round(row['tb_longitude'], 3)}_{round(row['tb_latitude'], 3)}",
                axis=1
            )

        # E. Create intersection flag if not already present
        if 'intersection' in records_df.columns:
            # Standardize values
            records_df['is_intersection'] = records_df['intersection'].str.upper().isin(['Y', 'YES', 'TRUE', '1'])
        elif 'primary_rd' in records_df.columns and 'secondary_rd' in records_df.columns:
            # Infer intersection status from presence of secondary road
            records_df['is_intersection'] = records_df['secondary_rd'].notna() & (records_df['secondary_rd'] != '')

        # 6. DATA VALIDATION - Identify logical inconsistencies
        # A. Validate collision severity matches casualties
        if all(col in records_df.columns for col in ['collision_severity', 'number_killed', 'number_injured']):
            # Check for fatal collisions not marked as such
            fatal_severity_mismatch = (
                    (records_df['number_killed'] > 0) &
                    (~records_df['collision_severity'].str.contains('Fatal', case=False, na=False))
            )

            for idx in records_df[fatal_severity_mismatch].index:
                record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                severity = records_df.loc[idx, 'collision_severity']
                killed = records_df.loc[idx, 'number_killed']
                self.log_error('collision_severity',
                               f"Severity inconsistency: marked as '{severity}' but has {killed} fatalities",
                               record_id, 2001)

        # B. Validate pedestrian information consistency
        if all(col in records_df.columns for col in ['ped_action', 'party1_type', 'party2_type']):
            # Check for pedestrian action without pedestrian involvement
            ped_inconsistency = (
                    (records_df['ped_action'] != 'Unknown') &
                    (records_df['ped_action'].notna()) &
                    (~records_df['party1_type'].str.contains('Pedestrian', case=False, na=False)) &
                    (~records_df['party2_type'].str.contains('Pedestrian', case=False, na=False))
            )

            for idx in records_df[ped_inconsistency].index:
                record_id = records_df.loc[idx, 'unique_id'] if 'unique_id' in records_df.columns else idx
                ped_action = records_df.loc[idx, 'ped_action']
                party1 = records_df.loc[idx, 'party1_type']
                party2 = records_df.loc[idx, 'party2_type']
                # self.log_error('ped_action',
                #               f"Pedestrian action '{ped_action}' but no pedestrian in parties: {party1}, {party2}",
                #               record_id, 2002)

        # Log completion of transform stage
        logger.info(f"Transform completed: {len(records_df)} records processed with {len(records_df.columns)} columns")

        return records_df


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    raw_file_path = current_dir.parent / "resources" / "raw" / "TrafficCrashes.csv"

    etl_job = TrafficCrashesETL(
        file_path=raw_file_path,
        file_category="SanFrancisco",
    )
    etl_job.run()
