from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler

from src.core.base_etl import BaseETLPipeline, logger


class AutismPatientETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        """
        Clean and transform the Autism Patient dataset.

        Data cleansing and transformation decisions:
        1. Handle missing values with appropriate strategies
        2. Correct data type inconsistencies
        3. Address outliers and extreme values
        4. Identify and handle duplicate records
        5. Encode categorical variables
        6. Create derived features and clinical metrics
        7. Normalize and standardize scores
        """
        # Store original dataframe for validation
        original_df = records_df.copy()

        # 1. HANDLING MISSING VALUES
        # A. Log missing values for reporting
        for column in records_df.columns:
            missing_count = records_df[column].isna().sum()
            if missing_count > 0:
                missing_pct = missing_count / len(records_df) * 100
                # Comment out as this is a summary log, not row-specific
                # self.log_error(column, f"Missing values: {missing_count} ({missing_pct:.2f}%)", None, 1001)

        # B. Handle missing values in critical identifier columns
        if 'ID' in records_df.columns and records_df['ID'].isna().any():
            missing_ids = records_df[records_df['ID'].isna()].index
            for idx in missing_ids:
                self.log_error('ID', f"Missing required identifier", idx, 1002,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))

        # C. Handle missing values in demographic data
        demographic_cols = ['Age', 'Gender', 'Ethnicity', 'Country', 'Diagnosis Age']
        for col in demographic_cols:
            if col in records_df.columns and records_df[col].isna().any():
                if col in ['Age', 'Diagnosis Age']:
                    # For age columns, use median imputation
                    median_value = records_df[col].median()
                    records_df[col] = records_df[col].fillna(median_value)
                    # Comment out as this is a summary log, not row-specific
                    # self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with median: {median_value}", None, 1003)
                else:
                    # For categorical demographics, use 'Unknown'
                    records_df[col] = records_df[col].fillna('Unknown')
                    # Comment out as this is a summary log, not row-specific
                    # self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with 'Unknown'", None, 1004)

        # D. Handle missing values in clinical assessment scores
        score_cols = ['IQ Score', 'Social Skills Score', 'Adaptive Behavior Score']
        if any(col in records_df.columns for col in score_cols):
            # First create a copy of the original columns for logging
            original_scores = {col: records_df[col].copy() for col in score_cols if col in records_df.columns}

            # Use KNN imputation for score columns
            score_cols_present = [col for col in score_cols if col in records_df.columns]
            if score_cols_present:
                try:
                    imputer = KNNImputer(n_neighbors=5)
                    records_df[score_cols_present] = imputer.fit_transform(records_df[score_cols_present])

                    # Log which values were imputed
                    for col in score_cols_present:
                        imputed_count = original_scores[col].isna().sum()
                        if imputed_count > 0:
                            # Comment out as this is a summary log, not row-specific
                            # self.log_error(col, f"KNN imputed {imputed_count} missing values", None, 1005)
                            pass
                except Exception as e:
                    # Comment out as this is a general error, not row-specific
                    # self.log_error('Score Imputation', f"Error during KNN imputation: {str(e)}", None, 1006)

                    # Fallback to median imputation if KNN fails
                    for col in score_cols_present:
                        if records_df[col].isna().any():
                            median_value = original_scores[col].median()
                            records_df[col] = records_df[col].fillna(median_value)
                            # Comment out as this is a summary log, not row-specific
                            # self.log_error(col, f"Fallback: Filled {original_scores[col].isna().sum()} missing values with median", None, 1007)

        # E. Handle missing values in symptom indicators
        symptom_cols = [
            'Language Development Delays',
            'Sensory Sensitivities',
            'Non-verbal Communication',
            'Repetitive Behaviors',
            'Sleep Disturbances',
        ]

        for col in symptom_cols:
            if col in records_df.columns and records_df[col].isna().any():
                # Use mode (most common value) for symptom columns
                mode_value = records_df[col].mode()[0]
                records_df[col] = records_df[col].fillna(mode_value)
                # Comment out as this is a summary log, not row-specific
                # self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with mode: {mode_value}", None, 1008)

        # F. Handle missing values in other categorical columns
        categorical_cols = [
            'Autism Severity',
            'Family History of Autism',
            'Parental Education Level',
            'Special Education Services',
            'Therapy Received',
            'Dietary Restrictions',
            'Comorbidities',
            'Current School Type',
            'Favorite Activities',
            'Support Needs'
        ]

        for col in categorical_cols:
            if col in records_df.columns and records_df[col].isna().any():
                # Use 'Unknown' for missing categorical data
                records_df[col] = records_df[col].fillna('Unknown')
                # Comment out as this is a summary log, not row-specific
                # self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with 'Unknown'", None, 1009)

        # 2. DATA TYPE CONVERSION
        # A. Ensure numeric columns have correct types
        numeric_type_conversions = {
            'ID': 'int',
            'Age': 'int',
            'Diagnosis Age': 'int',
            'IQ Score': 'int',
            'Social Skills Score': 'int',
            'Adaptive Behavior Score': 'int'
        }

        for col, data_type in numeric_type_conversions.items():
            if col in records_df.columns:
                try:
                    if data_type == 'int':
                        records_df[col] = records_df[col].round().astype(int)
                except Exception as e:
                    # Comment out as this is a general error, not row-specific
                    # self.log_error(col, f"Error converting to {data_type}: {str(e)}", None, 1010)
                    pass

        # B. Validate and correct age-related fields
        if 'Age' in records_df.columns and 'Diagnosis Age' in records_df.columns:
            # Ensure diagnosis age is not greater than current age
            invalid_ages = records_df[records_df['Diagnosis Age'] > records_df['Age']].index
            for idx in invalid_ages:
                age = records_df.loc[idx, 'Age']
                diag_age = records_df.loc[idx, 'Diagnosis Age']
                self.log_error('Diagnosis Age', f"Diagnosis age ({diag_age}) > current age ({age})", idx, 1011,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))
                # Correct by setting diagnosis age equal to current age
                records_df.loc[idx, 'Diagnosis Age'] = records_df.loc[idx, 'Age']

        # 3. OUTLIER MANAGEMENT
        # A. Handle outliers in IQ Score
        if 'IQ Score' in records_df.columns:
            # IQ score typically ranges from 40-160
            iq_min, iq_max = 40, 160

            # Log outliers
            low_outliers = records_df[records_df['IQ Score'] < iq_min].index
            high_outliers = records_df[records_df['IQ Score'] > iq_max].index

            for idx in low_outliers:
                iq_value = records_df.loc[idx, 'IQ Score']
                self.log_error('IQ Score', f"Low outlier detected: {iq_value} < {iq_min}", idx, 1012,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))

            for idx in high_outliers:
                iq_value = records_df.loc[idx, 'IQ Score']
                self.log_error('IQ Score', f"High outlier detected: {iq_value} > {iq_max}", idx, 1013,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))

            # Cap outliers
            records_df['IQ_Score_Capped'] = np.clip(records_df['IQ Score'], iq_min, iq_max)

            outlier_count = len(low_outliers) + len(high_outliers)
            if outlier_count > 0:
                # Comment out as this is a summary log, not row-specific
                # self.log_error('IQ Score', f"Capped {outlier_count} outliers to range [{iq_min}, {iq_max}]", None, 1014)
                pass

        # B. Handle outliers in Adaptive Behavior Score
        if 'Adaptive Behavior Score' in records_df.columns:
            # Determine reasonable bounds based on data distribution
            Q1 = records_df['Adaptive Behavior Score'].quantile(0.25)
            Q3 = records_df['Adaptive Behavior Score'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            # Log outliers
            low_outliers = records_df[records_df['Adaptive Behavior Score'] < lower_bound].index
            high_outliers = records_df[records_df['Adaptive Behavior Score'] > upper_bound].index

            for idx in low_outliers:
                score_value = records_df.loc[idx, 'Adaptive Behavior Score']
                self.log_error('Adaptive Behavior Score', f"Low outlier detected: {score_value} < {lower_bound}", idx,
                               1015,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))

            for idx in high_outliers:
                score_value = records_df.loc[idx, 'Adaptive Behavior Score']
                self.log_error('Adaptive Behavior Score', f"High outlier detected: {score_value} > {upper_bound}", idx,
                               1016,
                               ", ".join(str(val) for val in records_df.iloc[idx].values))

            # Cap outliers
            records_df['Adaptive_Behavior_Score_Capped'] = np.clip(
                records_df['Adaptive Behavior Score'],
                lower_bound,
                upper_bound
            )

            outlier_count = len(low_outliers) + len(high_outliers)
            if outlier_count > 0:
                # Comment out as this is a summary log, not row-specific
                # self.log_error('Adaptive Behavior Score', f"Capped {outlier_count} outliers to range [{lower_bound:.1f}, {upper_bound:.1f}]", None, 1017)
                pass

        # 4. DUPLICATE DETECTION
        # Check for duplicate patient records
        if 'ID' in records_df.columns:
            # Get duplicate IDs
            duplicate_ids = records_df[records_df.duplicated('ID', keep='first')]['ID']

            if not duplicate_ids.empty:
                for dup_id in duplicate_ids:
                    # Find the rows with this duplicate ID (excluding the first one)
                    dup_rows = records_df[records_df['ID'] == dup_id].iloc[1:]
                    for idx in dup_rows.index:
                        self.log_error('ID', f"Duplicate patient ID: {dup_id}", idx, 1018,
                                       ", ".join(str(val) for val in records_df.iloc[idx].values))

                # Keep only first occurrence of each ID
                records_df.drop_duplicates(subset=['ID'], keep='first', inplace=True)
                # Comment out as this is a summary log, not row-specific
                # self.log_error('Duplicate Records', f"Removed {len(duplicate_ids)} duplicate patient records", None, 1019)

        # 5. FEATURE ENGINEERING
        # A. Map Autism Severity to numeric scale
        if 'Autism Severity' in records_df.columns:
            severity_map = {'Mild': 1, 'Moderate': 2, 'Severe': 3, 'Unknown': 0}
            records_df['Severity_of_Symptoms'] = records_df['Autism Severity'].map(severity_map)

            # Handle any unmapped values
            unmapped = records_df[~records_df['Autism Severity'].isin(severity_map.keys())]['Autism Severity'].unique()
            if len(unmapped) > 0:
                # Comment out as this is a general mapping, not row-specific
                # self.log_error('Autism Severity', f"Unmapped severity values: {unmapped}", None, 1020)

                # Set unmapped values to 0 (Unknown)
                records_df.loc[~records_df['Autism Severity'].isin(severity_map.keys()), 'Severity_of_Symptoms'] = 0

        # B. Count number of symptoms marked "Yes"
        symptom_cols = [
            'Language Development Delays',
            'Sensory Sensitivities',
            'Non-verbal Communication',
            'Repetitive Behaviors',
            'Sleep Disturbances'
        ]

        if all(col in records_df.columns for col in symptom_cols):
            records_df['Number_of_Symptoms'] = records_df[symptom_cols].apply(
                lambda row: sum(val == 'Yes' for val in row),
                axis=1
            )

        # C. Create symptom profile categories
        if 'Number_of_Symptoms' in records_df.columns:
            # Define symptom profile categories
            def get_symptom_profile(num_symptoms):
                if num_symptoms <= 1:
                    return 'Minimal'
                elif num_symptoms <= 3:
                    return 'Moderate'
                else:
                    return 'Comprehensive'

            records_df['Symptom_Profile'] = records_df['Number_of_Symptoms'].apply(get_symptom_profile)

        # D. Calculate time since diagnosis
        if 'Age' in records_df.columns and 'Diagnosis Age' in records_df.columns:
            records_df['Years_Since_Diagnosis'] = records_df['Age'] - records_df['Diagnosis Age']

        # E. Rename and preserve original score columns
        if 'IQ Score' in records_df.columns:
            records_df['Initial_Score'] = records_df['IQ Score']

        if 'Adaptive Behavior Score' in records_df.columns:
            records_df['Latest_Score'] = records_df['Adaptive Behavior Score']

        # F. Generate number of therapy sessions (simulated)
        np.random.seed(42)  # For reproducibility
        records_df['Number_of_Sessions'] = np.random.randint(5, 21, size=len(records_df))

        # G. Calculate therapy effectiveness
        if all(col in records_df.columns for col in ['Initial_Score', 'Latest_Score', 'Number_of_Sessions']):
            records_df['Therapy_Effectiveness'] = (
                    (records_df['Latest_Score'] - records_df['Initial_Score']) / records_df['Number_of_Sessions']
            )

        # H. Create early intervention flag
        if 'Age' in records_df.columns:
            records_df['Needs_High_Intensity_Intervention'] = records_df['Age'].apply(
                lambda age: 'Yes' if age < 6 else 'No'
            )

        # I. Create comorbidity count
        if 'Comorbidities' in records_df.columns:
            # Count comorbidities separated by commas
            records_df['Comorbidity_Count'] = records_df['Comorbidities'].apply(
                lambda x: len(str(x).split(',')) if pd.notna(x) and x != 'None' and x != 'Unknown' else 0
            )

        # 6. CATEGORICAL ENCODING
        # A. Map Support Needs to Risk Level codes
        if 'Support Needs' in records_df.columns:
            risk_level_mapping = {'High': 'HR', 'Medium': 'MR', 'Low': 'LR', 'Unknown': 'UN'}
            records_df['Risk_Level'] = records_df['Support Needs']
            records_df['Risk_Level_Transformed'] = records_df['Risk_Level'].map(risk_level_mapping)

            # Handle any unmapped values
            unmapped = records_df[~records_df['Support Needs'].isin(risk_level_mapping.keys())][
                'Support Needs'].unique()
            if len(unmapped) > 0:
                # Comment out as this is a general mapping, not row-specific
                # self.log_error('Support Needs', f"Unmapped support need values: {unmapped}", None, 1021)

                # Set unmapped values to 'UN' (Unknown)
                records_df.loc[
                    ~records_df['Support Needs'].isin(risk_level_mapping.keys()), 'Risk_Level_Transformed'] = 'UN'

        # B. Encode Family History
        if 'Family History of Autism' in records_df.columns:
            family_history_map = {'Yes': 1, 'No': 0, 'Unknown': -1}
            records_df['Family_History_Encoded'] = records_df['Family History of Autism'].map(family_history_map)

            # Handle unmapped values
            unmapped = records_df[~records_df['Family History of Autism'].isin(family_history_map.keys())][
                'Family History of Autism'].unique()
            if len(unmapped) > 0:
                # Comment out as this is a general mapping, not row-specific
                # self.log_error('Family History of Autism', f"Unmapped family history values: {unmapped}", None, 1022)

                # Set unmapped values to -1 (Unknown)
                records_df.loc[~records_df['Family History of Autism'].isin(
                    family_history_map.keys()), 'Family_History_Encoded'] = -1

        # C. Encode Parental Education Level
        if 'Parental Education Level' in records_df.columns:
            education_map = {
                'High School': 1,
                'Some College': 2,
                'Bachelor\'s': 3,
                'Master\'s': 4,
                'Doctorate': 5,
                'Unknown': 0
            }
            records_df['Education_Level_Encoded'] = records_df['Parental Education Level'].map(education_map)

            # Handle unmapped values
            unmapped = records_df[~records_df['Parental Education Level'].isin(education_map.keys())][
                'Parental Education Level'].unique()
            if len(unmapped) > 0:
                # Comment out as this is a general mapping, not row-specific
                # self.log_error('Parental Education Level', f"Unmapped education values: {unmapped}", None, 1023)

                # Set unmapped values to 0 (Unknown)
                records_df.loc[
                    ~records_df['Parental Education Level'].isin(education_map.keys()), 'Education_Level_Encoded'] = 0

        # 7. STANDARDIZATION AND NORMALIZATION
        # A. Standardize score columns
        score_cols_for_standard = ['IQ Score', 'Social Skills Score', 'Adaptive Behavior Score']
        available_score_cols = [col for col in score_cols_for_standard if col in records_df.columns]

        if available_score_cols:
            standardizer = StandardScaler()

            # Create standardized columns
            standardized_data = standardizer.fit_transform(records_df[available_score_cols])

            # Add standardized columns to DataFrame
            for i, col in enumerate(available_score_cols):
                std_col_name = f"{col.replace(' ', '_')}_Standardized"
                records_df[std_col_name] = standardized_data[:, i]

        # B. Normalize age values to 0-1 scale
        age_cols = ['Age', 'Diagnosis Age']
        available_age_cols = [col for col in age_cols if col in records_df.columns]

        if available_age_cols:
            normalizer = MinMaxScaler()

            # Create normalized columns
            normalized_data = normalizer.fit_transform(records_df[available_age_cols])

            # Add normalized columns to DataFrame
            for i, col in enumerate(available_age_cols):
                norm_col_name = f"{col.replace(' ', '_')}_Normalized"
                records_df[norm_col_name] = normalized_data[:, i]

        # 8. Create ETL processing metadata
        current_time = datetime.now()
        records_df['ETL_Processed_Date'] = current_time.strftime('%Y-%m-%d')
        records_df['ETL_Processed_Timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

        # Log completion of transform stage
        logger.info(f"Transform completed: {len(records_df)} records processed with {len(records_df.columns)} columns")

        return records_df


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    raw_file_path = current_dir.parent / "resources" / "raw" / "autism_patient.csv"

    etl_job = AutismPatientETL(
        file_path=raw_file_path,
        file_category="AutismPatientData",
    )
    etl_job.run()