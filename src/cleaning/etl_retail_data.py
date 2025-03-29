from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.impute import SimpleImputer

from src.core.base_etl import BaseETLPipeline, logger


class RetailDataETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        """
        Clean and transform the Retail sales dataset.

        Data cleansing and transformation decisions:
        1. Handle missing values through imputation strategies
        2. Correct data type inconsistencies
        3. Address outliers using capping technique
        4. Fix date and time fields
        5. Create derived features and aggregated metrics
        6. Apply normalization and standardization
        7. Encode categorical variables
        """
        # Store original dataframe for validation
        original_df = records_df.copy()

        # 1. HANDLING MISSING VALUES
        # A. First, log missing values for reporting
        for column in records_df.columns:
            missing_count = records_df[column].isna().sum()
            if missing_count > 0:
                missing_pct = missing_count / len(records_df) * 100
                self.log_error(column, f"Missing values: {missing_count} ({missing_pct:.2f}%)", None, 1001)

        # B. Handle missing values in date/time columns
        date_cols = ['Year', 'Month', 'Day', 'Hour']
        for col in date_cols:
            if col in records_df.columns:
                # Log records with missing dates
                missing_dates = records_df[records_df[col].isna()].index
                for idx in missing_dates:
                    record_id = idx
                    self.log_error(col, f"Missing {col} value", record_id, 1002)

                # Fill with reasonable defaults for processing
                records_df[col] = records_df[col].fillna(0).astype(int)

        # C. Handle missing values in numeric columns using median imputation
        numeric_cols = records_df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            imputer = SimpleImputer(strategy='median')
            records_df[numeric_cols] = imputer.fit_transform(records_df[numeric_cols])

            # Log which columns were imputed
            for col in numeric_cols:
                if original_df[col].isna().sum() > 0:
                    self.log_error(col, f"Imputed {original_df[col].isna().sum()} missing values with median", None,
                                   1003)

        # D. Handle missing values in categorical columns
        categorical_cols = records_df.select_dtypes(include=['object']).columns.tolist()
        for col in categorical_cols:
            if records_df[col].isna().any():
                # Fill with 'Unknown'
                records_df[col] = records_df[col].fillna('Unknown')
                self.log_error(col, f"Filled {original_df[col].isna().sum()} missing values with 'Unknown'", None, 1004)

        # 2. DATA TYPE CONVERSION
        # A. Convert date fields to correct types
        records_df = records_df.astype({"Year": int, "Month": int, "Day": int, "Hour": int})
        if all(col in records_df.columns for col in date_cols):
            try:
                # Create datetime field from components
                records_df['Datetime'] = pd.to_datetime(
                    records_df['Year'].astype(str) + '-' +
                    records_df['Month'].astype(str).str.zfill(2) + '-' +
                    records_df['Day'].astype(str).str.zfill(2) + ' ' +
                    records_df['Hour'].astype(str).str.zfill(2),
                    format='%Y-%m-%d %H',
                    errors='coerce'
                )

                # Log records with invalid datetime
                invalid_datetime = records_df['Datetime'].isna()
                for idx in records_df[invalid_datetime].index:
                    record_id = idx
                    year = records_df.loc[idx, 'Year']
                    month = records_df.loc[idx, 'Month']
                    day = records_df.loc[idx, 'Day']
                    hour = records_df.loc[idx, 'Hour']
                    self.log_error('Datetime',
                                   f"Invalid datetime components: year={year}, month={month}, day={day}, hour={hour}",
                                   record_id, 1005)
            except Exception as e:
                self.log_error('Datetime', f"Error creating datetime field: {str(e)}", None, 1006)

        # B. Convert other fields to appropriate types
        type_conversions = {
            'Product_ID': 'str',
            'Customer_ID': 'str',
            'Transaction_ID': 'str',
            'Quantity': 'int',
            'Price': 'float',
            'Cost': 'float',
            'Profit_Margin': 'float',
        }

        for col, data_type in type_conversions.items():
            if col in records_df.columns:
                try:
                    if data_type == 'int':
                        records_df[col] = records_df[col].fillna(0).astype(int)
                    elif data_type == 'float':
                        records_df[col] = records_df[col].fillna(0).astype(float)
                    elif data_type == 'str':
                        records_df[col] = records_df[col].fillna('').astype(str)
                except Exception as e:
                    self.log_error(col, f"Error converting to {data_type}: {str(e)}", None, 1007)

        # 3. OUTLIER MANAGEMENT
        # Handle outliers in Cost using IQR method
        if 'Cost' in records_df.columns:
            Q1 = records_df['Cost'].quantile(0.25)
            Q3 = records_df['Cost'].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 1.5 * IQR

            # Log outliers
            outliers = records_df[records_df['Cost'] > upper_bound]
            for idx in outliers.index:
                record_id = idx
                cost_value = records_df.loc[idx, 'Cost']
                self.log_error('Cost', f"Outlier detected: {cost_value} > {upper_bound}", record_id, 1008)

            # Cap outliers
            records_df['Cost_Capped'] = np.where(
                records_df['Cost'] > upper_bound,
                upper_bound,
                records_df['Cost']
            )

            if len(outliers) > 0:
                self.log_error('Cost', f"Capped {len(outliers)} outliers above {upper_bound}", None, 1009)

        # Handle outliers in Price using similar method
        if 'Price' in records_df.columns:
            Q1 = records_df['Price'].quantile(0.25)
            Q3 = records_df['Price'].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 1.5 * IQR

            # Log outliers
            outliers = records_df[records_df['Price'] > upper_bound]
            for idx in outliers.index:
                record_id = idx
                price_value = records_df.loc[idx, 'Price']
                self.log_error('Price', f"Outlier detected: {price_value} > {upper_bound}", record_id, 1010)

            # Cap outliers
            records_df['Price_Capped'] = np.where(
                records_df['Price'] > upper_bound,
                upper_bound,
                records_df['Price']
            )

            if len(outliers) > 0:
                self.log_error('Price', f"Capped {len(outliers)} outliers above {upper_bound}", None, 1011)

        # 4. DUPLICATE DETECTION
        # Check for duplicate transactions
        if 'Transaction_ID' in records_df.columns:
            duplicate_transactions = records_df[records_df.duplicated('Transaction_ID', keep='first')]
            for idx in duplicate_transactions.index:
                record_id = idx
                transaction_id = records_df.loc[idx, 'Transaction_ID']
                self.log_error('Transaction_ID', f"Duplicate transaction: {transaction_id}", record_id, 1012)

        # 5. FEATURE ENGINEERING
        # A. Create time-based features if datetime is available
        if 'Datetime' in records_df.columns:
            # Extract components
            records_df['Day_of_Week'] = records_df['Datetime'].dt.dayofweek
            records_df['Is_Weekend'] = records_df['Day_of_Week'].isin([5, 6])  # 5=Sat, 6=Sun
            records_df['Day_Name'] = records_df['Datetime'].dt.day_name()
            records_df['Month_Name'] = records_df['Datetime'].dt.month_name()
            records_df['Quarter'] = records_df['Datetime'].dt.quarter

            # Create time of day category
            def categorize_time(hour):
                if pd.isna(hour):
                    return 'Unknown'
                elif 0 <= hour < 6:
                    return 'Early Morning'
                elif 6 <= hour < 12:
                    return 'Morning'
                elif 12 <= hour < 18:
                    return 'Afternoon'
                else:
                    return 'Evening'

            if 'Hour' in records_df.columns:
                records_df['Time_of_Day'] = records_df['Hour'].apply(categorize_time)

        # B. Create customer age groups if customer age is available
        if 'Customer_Age' in records_df.columns:
            # Define age bins and labels
            bins = [0, 18, 25, 35, 45, 55, 65, 100]
            labels = ['0-18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']

            # Create age groups
            records_df['Age_Group'] = pd.cut(
                records_df['Customer_Age'],
                bins=bins,
                labels=labels,
                right=False
            )

        # C. Create product category aggregations
        # Sum sales across product categories
        electronics_cols = [col for col in records_df.columns if col.startswith('Electronics_')]
        if electronics_cols:
            records_df['Total_Electronics_Sales'] = records_df[electronics_cols].sum(axis=1)

        clothing_cols = [col for col in records_df.columns if col.startswith('Clothing_')]
        if clothing_cols:
            records_df['Total_Clothing_Sales'] = records_df[clothing_cols].sum(axis=1)

        # D. Create revenue and profit fields
        if all(col in records_df.columns for col in ['Price', 'Quantity']):
            records_df['Revenue'] = records_df['Price'] * records_df['Quantity']

            if 'Cost' in records_df.columns:
                records_df['Profit'] = records_df['Revenue'] - (records_df['Cost'] * records_df['Quantity'])
                records_df['Profit_Percentage'] = (records_df['Profit'] / records_df['Revenue'] * 100).round(2)

        # 6. NORMALIZATION AND STANDARDIZATION
        # A. Normalize Cost in a new column
        if 'Cost' in records_df.columns:
            scaler = MinMaxScaler()
            records_df['Cost_Normalized'] = scaler.fit_transform(records_df[['Cost']])

        # B. Standardize Profit Margin in a new column
        if 'Profit_Margin' in records_df.columns:
            standard_scaler = StandardScaler()
            records_df['Profit_Margin_Standardized'] = standard_scaler.fit_transform(records_df[['Profit_Margin']])

        # 7. CATEGORICAL ENCODING
        # A. Encode Customer Region
        if 'Customer_Region' in records_df.columns:
            records_df['Customer_Region_Encoded'] = pd.Categorical(records_df['Customer_Region']).codes

            # Create region mapping for reference
            region_mapping = {code: region for code, region in
                              zip(pd.Categorical(records_df['Customer_Region']).codes,
                                  pd.Categorical(records_df['Customer_Region']).categories)}

            # Log the mapping for reference
            self.log_error('Customer_Region', f"Region mapping created: {region_mapping}", None, 2001)

        # B. Encode Product Category
        if 'Product_Category' in records_df.columns:
            records_df['Product_Category_Encoded'] = pd.Categorical(records_df['Product_Category']).codes

            # Create category mapping for reference
            category_mapping = {code: category for code, category in
                                zip(pd.Categorical(records_df['Product_Category']).codes,
                                    pd.Categorical(records_df['Product_Category']).categories)}

            # Log the mapping for reference
            self.log_error('Product_Category', f"Category mapping created: {category_mapping}", None, 2002)

        # 8. Create ETL processing metadata
        current_time = datetime.now()
        records_df['ETL_Processed_Date'] = current_time.strftime('%Y-%m-%d')
        records_df['ETL_Processed_Timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

        # Log completion of transform stage
        logger.info(f"Transform completed: {len(records_df)} records processed with {len(records_df.columns)} columns")

        return records_df


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    raw_file_path = current_dir.parent / "resources" / "raw" / "Retail_Data1.csv"

    etl_job = RetailDataETL(
        file_path=raw_file_path,
        file_category="RetailSales",
    )
    etl_job.run()