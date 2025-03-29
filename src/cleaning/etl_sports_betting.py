from pathlib import Path
import logging

import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from src.core.base_etl import BaseETLPipeline

logger = logging.getLogger(__name__)


class SportsBettingETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        """
        Clean and transform the Sports Betting User Behavior dataset.

        Data cleansing and transformation decisions:
        1. Handle missing values with appropriate strategies
        2. Correct data type inconsistencies
        3. Address outliers and extreme values
        4. Identify and handle duplicate records
        5. Transform timestamps and calculate time-based metrics
        6. Normalize numeric fields and encode categorical variables
        7. Create derived features to enhance analysis
        """
        # Store original dataframe for validation
        original_df = records_df.copy()

        # 1. HANDLING MISSING VALUES
        # A. Log missing values for reporting
        for column in records_df.columns:
            missing_count = records_df[column].isna().sum()
            if missing_count > 0:
                missing_pct = missing_count / len(records_df) * 100
                self.log_error(column, f"Missing values: {missing_count} ({missing_pct:.2f}%)", None, 1001)

        # B. Fill missing values in critical columns
        # Essential identifiers - cannot be null
        if 'User ID' in records_df.columns and records_df['User ID'].isna().any():
            missing_ids = records_df[records_df['User ID'].isna()].index
            for idx in missing_ids:
                self.log_error('User ID', f"Missing required identifier", idx, 1002)

        # Handle timestamp nulls
        if 'Timestamp' in records_df.columns and records_df['Timestamp'].isna().any():
            missing_timestamps = records_df[records_df['Timestamp'].isna()].index
            for idx in missing_timestamps:
                self.log_error('Timestamp', f"Missing timestamp", idx, 1003)

        # Handle missing numeric values
        numeric_cols = ['Session Duration', 'Amount Spent', 'Active Time', 'Bet Amount', 'Odds']
        for col in numeric_cols:
            if col in records_df.columns and records_df[col].isna().any():
                # Different strategies based on column type
                if col in ['Session Duration', 'Active Time']:
                    # Use median for time-related fields
                    median_value = records_df[col].median()
                    records_df[col] = records_df[col].fillna(median_value)
                    self.log_error(col,
                                   f"Filled {original_df[col].isna().sum()} missing values with median: {median_value}",
                                   None, 1004)
                elif col in ['Amount Spent', 'Bet Amount']:
                    # Use 0 for financial fields (assuming no bet was placed)
                    records_df[col] = records_df[col].fillna(0)
                    self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with 0", None, 1005)
                elif col == 'Odds':
                    # Use median for odds
                    median_value = records_df[col].median()
                    records_df[col] = records_df[col].fillna(median_value)
                    self.log_error(col,
                                   f"Filled {original_df[col].isna().sum()} missing values with median: {median_value}",
                                   None, 1006)

        # Handle missing categorical values
        categorical_cols = ['Device Type', 'Page Name', 'Actions Taken', 'Betting History',
                            'Geolocation', 'Device Language', 'Referral Source', 'User Type',
                            'Win/Loss Status', 'Bet Type', 'Sport Type', 'Login Frequency',
                            'Page Navigation']

        for col in categorical_cols:
            if col in records_df.columns and records_df[col].isna().any():
                # Fill with 'Unknown'
                records_df[col] = records_df[col].fillna('Unknown')
                self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with 'Unknown'", None, 1007)

        # Handle missing boolean values
        boolean_cols = ['Push Notifications', 'User Engagement', 'Coupon Usage',
                        'Error Logs', 'Help Interaction', 'Conversion Rate']

        for col in boolean_cols:
            if col in records_df.columns and records_df[col].isna().any():
                # Fill with False (assuming no action was taken)
                records_df[col] = records_df[col].fillna(False)
                self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with False", None, 1008)

        # 2. DATA TYPE CONVERSION
        # A. Convert timestamp to datetime
        if 'Timestamp' in records_df.columns:
            try:
                records_df['Timestamp'] = pd.to_datetime(records_df['Timestamp'], errors='coerce')

                # Log records with invalid datetime
                invalid_timestamps = records_df[records_df['Timestamp'].isna()].index
                for idx in invalid_timestamps:
                    original_value = original_df.loc[idx, 'Timestamp']
                    self.log_error('Timestamp', f"Invalid timestamp: {original_value}", idx, 1009)
            except Exception as e:
                self.log_error('Timestamp', f"Error converting timestamps: {str(e)}", None, 1010)

        # B. Ensure numeric columns have correct types
        numeric_type_conversions = {
            'User ID': 'int',
            'Session Duration': 'float',
            'Amount Spent': 'float',
            'Active Time': 'float',
            'Bet Amount': 'float',
            'Odds': 'float',
            'Session Frequency': 'int'
        }

        for col, data_type in numeric_type_conversions.items():
            if col in records_df.columns:
                try:
                    if data_type == 'int':
                        records_df[col] = records_df[col].fillna(0).astype(int)
                    elif data_type == 'float':
                        records_df[col] = records_df[col].fillna(0).astype(float)
                except Exception as e:
                    self.log_error(col, f"Error converting to {data_type}: {str(e)}", None, 1011)

        # C. Ensure boolean columns have correct types
        for col in boolean_cols:
            if col in records_df.columns:
                try:
                    # Handle various boolean representations
                    if records_df[col].dtype != bool:
                        # Convert string representations to bool
                        true_values = ['true', 'yes', 'y', '1', 't', 'True', 'TRUE']
                        false_values = ['false', 'no', 'n', '0', 'f', 'False', 'FALSE']

                        # For string columns
                        if records_df[col].dtype == 'object':
                            # Convert known values, leave others as is
                            records_df[col] = records_df[col].apply(
                                lambda x: True if str(x).lower() in true_values else
                                False if str(x).lower() in false_values else x
                            )

                        # Final conversion to bool
                        records_df[col] = records_df[col].astype(bool)
                except Exception as e:
                    self.log_error(col, f"Error converting to boolean: {str(e)}", None, 1012)

        # 3. OUTLIER MANAGEMENT
        # A. Handle outliers in Session Duration
        if 'Session Duration' in records_df.columns:
            Q1 = records_df['Session Duration'].quantile(0.25)
            Q3 = records_df['Session Duration'].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 1.5 * IQR

            # Log outliers
            outliers = records_df[records_df['Session Duration'] > upper_bound]
            for idx in outliers.index:
                duration_value = records_df.loc[idx, 'Session Duration']
                self.log_error('Session Duration', f"Outlier detected: {duration_value} > {upper_bound}", idx, 1013)

            # Cap outliers
            records_df['Session_Duration_Capped'] = np.where(
                records_df['Session Duration'] > upper_bound,
                upper_bound,
                records_df['Session Duration']
            )

            if len(outliers) > 0:
                self.log_error('Session Duration', f"Capped {len(outliers)} outliers above {upper_bound}", None, 1014)

        # B. Handle outliers in Bet Amount
        if 'Bet Amount' in records_df.columns:
            Q1 = records_df['Bet Amount'].quantile(0.25)
            Q3 = records_df['Bet Amount'].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 1.5 * IQR

            # Log outliers
            outliers = records_df[records_df['Bet Amount'] > upper_bound]
            for idx in outliers.index:
                bet_value = records_df.loc[idx, 'Bet Amount']
                self.log_error('Bet Amount', f"Outlier detected: {bet_value} > {upper_bound}", idx, 1015)

            # Cap outliers
            records_df['Bet_Amount_Capped'] = np.where(
                records_df['Bet Amount'] > upper_bound,
                upper_bound,
                records_df['Bet Amount']
            )

            if len(outliers) > 0:
                self.log_error('Bet Amount', f"Capped {len(outliers)} outliers above {upper_bound}", None, 1016)

        # 4. DUPLICATE DETECTION
        # Check for duplicate user sessions (same User ID and Timestamp)
        if all(col in records_df.columns for col in ['User ID', 'Timestamp']):
            # Get duplicate rows
            duplicate_mask = records_df.duplicated(subset=['User ID', 'Timestamp'], keep='first')
            duplicate_rows = records_df[duplicate_mask]

            for idx in duplicate_rows.index:
                user_id = records_df.loc[idx, 'User ID']
                timestamp = records_df.loc[idx, 'Timestamp']
                self.log_error('User Session', f"Duplicate session: User ID {user_id} at {timestamp}", idx, 1017)

        # 5. FEATURE ENGINEERING
        # A. Timestamp transformations
        if 'Timestamp' in records_df.columns and 'Session Duration' in records_df.columns:
            # Calculate session end time
            records_df['End_Timestamp'] = records_df['Timestamp'] + pd.to_timedelta(records_df['Session Duration'],
                                                                                    unit='m')

            # Extract time components
            records_df['Hour'] = records_df['Timestamp'].dt.hour
            records_df['Day_of_Week'] = records_df['Timestamp'].dt.dayofweek
            records_df['Day_Name'] = records_df['Timestamp'].dt.day_name()
            records_df['Month'] = records_df['Timestamp'].dt.month
            records_df['Year'] = records_df['Timestamp'].dt.year
            records_df['Is_Weekend'] = records_df['Day_of_Week'].isin([5, 6])  # 5=Sat, 6=Sun

            # Create time of day category
            def categorize_time(hour):
                if pd.isna(hour):
                    return 'Unknown'
                elif 0 <= hour < 6:
                    return 'Night'
                elif 6 <= hour < 12:
                    return 'Morning'
                elif 12 <= hour < 18:
                    return 'Afternoon'
                else:
                    return 'Evening'

            records_df['Time_of_Day'] = records_df['Hour'].apply(categorize_time)

        # B. Normalize login frequency
        if 'Login Frequency' in records_df.columns:
            # Define a function to normalize to monthly scale
            def normalize_frequency(freq_str):
                if pd.isna(freq_str) or freq_str == 'Unknown':
                    return 0

                try:
                    # Parse frequency strings like "5 times per day", "2 times per week", etc.
                    parts = freq_str.lower().split()
                    if len(parts) >= 4 and 'per' in parts:
                        num = float(parts[0])
                        period = parts[3]  # day, week, month

                        if period == 'day':
                            return num * 30  # Assuming 30 days in a month
                        elif period == 'week':
                            return num * 4.3  # Assuming 4.3 weeks in a month
                        elif period == 'month':
                            return num
                        else:
                            return 0
                    else:
                        return 0
                except:
                    return 0

            records_df['Login_Frequency_Monthly'] = records_df['Login Frequency'].apply(normalize_frequency)

        # C. Create engagement score
        engagement_cols = ['Session Duration', 'Active Time', 'Session Frequency']
        if all(col in records_df.columns for col in engagement_cols):
            # Normalize each component
            scaler = MinMaxScaler()

            # Create normalized columns
            for col in engagement_cols:
                if records_df[col].std() > 0:  # Check if there's variation to normalize
                    norm_col = f"{col.replace(' ', '_')}_Normalized"
                    records_df[norm_col] = scaler.fit_transform(records_df[[col]])

            # Calculate engagement score (average of normalized values)
            normalized_cols = [f"{col.replace(' ', '_')}_Normalized" for col in engagement_cols
                               if f"{col.replace(' ', '_')}_Normalized" in records_df.columns]

            if normalized_cols:
                records_df['Engagement_Score'] = records_df[normalized_cols].mean(axis=1)

        # D. Create betting behavior features
        if all(col in records_df.columns for col in ['Bet Amount', 'Amount Spent']):
            # Calculate average bet size
            user_bets = records_df.groupby('User ID')['Bet Amount'].mean().reset_index()
            user_bets.columns = ['User ID', 'Average_Bet_Size']
            records_df = records_df.merge(user_bets, on='User ID', how='left')

            # Calculate cumulative amount spent for each user
            if 'Timestamp' in records_df.columns:
                # Sort by user and timestamp
                records_df = records_df.sort_values(['User ID', 'Timestamp'])

                # Calculate running total for each user
                records_df['Cumulative_Amount_Spent'] = records_df.groupby('User ID')['Amount Spent'].cumsum()

        # E. Parse and extract betting patterns
        if 'Betting History' in records_df.columns:
            # Extract betting patterns if available in structured format
            # This is placeholder logic - adjust based on actual data format
            try:
                # Check if betting history contains win/loss pattern info
                if records_df['Betting History'].str.contains('W|L').any():
                    # Calculate win ratio from history (assuming format like "WWLWL")
                    records_df['Win_Ratio'] = records_df['Betting History'].apply(
                        lambda x: x.count('W') / len(x) if isinstance(x, str) and len(x) > 0 else np.nan
                    )

                    # Calculate longest winning streak
                    records_df['Longest_Win_Streak'] = records_df['Betting History'].apply(
                        lambda x: max(len(streak) for streak in x.split('L') if streak) if isinstance(x, str) else 0
                    )
            except Exception as e:
                self.log_error('Betting History', f"Error parsing betting patterns: {str(e)}", None, 1018)

        # 6. CATEGORICAL ENCODING
        # A. Create mappings for categorical variables
        mappings = {
            'Device Type': {'Android': 'A', 'iOS': 'I', 'Web': 'W', 'Unknown': 'U'},
            'User Type': {'Returning': 'RT', 'New': 'NW', 'VIP': 'VP', 'Unknown': 'UN'},
            'Win/Loss Status': {'Win': 'WN', 'No Bet': 'NB', 'Loss': 'LS', 'Unknown': 'UN'},
        }

        # Apply mappings to create new encoded columns
        for column, mapping in mappings.items():
            if column in records_df.columns:
                transformed_col = f"{column.replace(' ', '_')}_Encoded"
                records_df[transformed_col] = records_df[column].map(mapping)

                # Fill any unmapped values
                if records_df[transformed_col].isna().any():
                    unmapped_values = records_df[records_df[transformed_col].isna()][column].unique()
                    self.log_error(column, f"Unmapped values found: {unmapped_values}", None, 1019)
                    records_df[transformed_col] = records_df[transformed_col].fillna('OT')  # Other

        # B. Create one-hot encoding for sport type
        if 'Sport Type' in records_df.columns:
            sport_dummies = pd.get_dummies(records_df['Sport Type'], prefix='Sport')
            records_df = pd.concat([records_df, sport_dummies], axis=1)

        # 7. STANDARDIZATION OF NUMERIC FIELDS
        # A. Standardize numeric fields for modeling
        numeric_fields_to_standardize = ['Bet Amount', 'Odds', 'Session Duration']
        standardizer = StandardScaler()

        for field in numeric_fields_to_standardize:
            if field in records_df.columns:
                std_field_name = f"{field.replace(' ', '_')}_Standardized"

                # Standardize non-missing values
                valid_mask = records_df[field].notna()
                if valid_mask.sum() > 0 and records_df.loc[valid_mask, field].std() > 0:
                    records_df.loc[valid_mask, std_field_name] = standardizer.fit_transform(
                        records_df.loc[valid_mask, [field]]
                    )

        # 8. Create ETL processing metadata
        current_time = datetime.now()
        records_df['ETL_Processed_Date'] = current_time.strftime('%Y-%m-%d')
        records_df['ETL_Processed_Timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

        # Log completion of transform stage
        logger.info(f"Transform completed: {len(records_df)} records processed with {len(records_df.columns)} columns")

        return records_df


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    raw_file_path = current_dir.parent / "resources" / "raw" / "SportsBettingUserBehavior.csv"

    etl_job = SportsBettingETL(
        file_path=raw_file_path,
        file_category="SportsBetting",
    )
    etl_job.run()