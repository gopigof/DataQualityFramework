import csv
import logging
from pathlib import Path

import pandas as pd

DATASET_FOLDER_CLEAN = "clean"
DATASET_FOLDER_NORMALIZED = "normalized"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class BaseNormalization:
    def __init__(self, file_path: str, dataset_name: str):
        self.file_path = self._normalize_file_paths(DATASET_FOLDER_CLEAN, file_path)
        self.normalized_folder_path = self._normalize_file_paths(DATASET_FOLDER_NORMALIZED, dataset_name)

    @staticmethod
    def _normalize_file_paths(folder_name: str, path: str) -> Path:
        fp = Path(__file__).parent.parent / "resources" / folder_name / path
        if not fp.is_file():
            fp.mkdir(exist_ok=True)
        return fp

    def load(self) -> pd.DataFrame:
        return pd.read_csv(self.file_path)

    def normalize(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        pass

    def write_data(self, dataframe_map: dict[str, pd.DataFrame]):
        for name, dataframe in dataframe_map.items():
            dataframe.to_csv(self.normalized_folder_path / f"{name}.csv", index=False)
            logger.info(f"Completed normalizing: {name}.csv")

    def run(self):
        try:
            df = self.load()
            normalized_datasets = self.normalize(df)
            self.write_data(normalized_datasets)
        except Exception as e:
            logger.exception(e)
