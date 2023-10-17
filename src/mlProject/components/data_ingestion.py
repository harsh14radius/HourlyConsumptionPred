import pandas as pd
from src.mlProject import logger
from src.mlProject.entity.config_entity import DataIngestionConfig
from src.mlProject.utils.common import get_mongoData


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def initiateDataIngestion(self):
        try:
            data = get_mongoData()
            logger.info("Reading Completed from MongoDB")
            logger.info("Writing data in Parquet file")
            data.to_parquet(self.config.local_data_file, index=False, engine='fastparquet')

        except Exception as e:
            print(f"Error in Ingestion Procss: {e}")



