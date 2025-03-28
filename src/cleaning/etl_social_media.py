from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.cleaning.base_etl import BaseETLPipeline


class SocialMediaETL(BaseETLPipeline):
    def transform(self, records_df: DataFrame):
        print(records_df.columns)

        # Data Cleansing

        # Data Transformation
        # Age Group: 4 years
        age_bins = list(range(0, 101, 5))
        age_labels = [f"{i}-{i + 4}" for i in range(0, 100, 5)]
        records_df["Age Group"] = pd.cut(records_df["Age"], bins=age_bins, labels=age_labels, right=False)

        # Gender - convert M, F, O
        records_df["GenderCode"] = records_df["Gender"].map({
            "Male": "M",
            "Female": "F",
            "Other": "O"
        })
        # Add validation

        return records_df


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    raw_file_path = current_dir.parent / "resources" / "raw" / "social_media_entertainment_data.csv"

    etl_job = SocialMediaETL(
        file_path=raw_file_path,
        file_category="SocialMedia",
    )
    etl_job.run()
