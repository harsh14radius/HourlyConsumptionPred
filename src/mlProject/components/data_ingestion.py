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
            i = 1
            for rec in data:
                logger.info("Reading Completed from MongoDB")
                logger.info("Writing data in Parquet file")
                filename = f"{self.config.local_data_file1}data{i}.parquet"
                rec.to_parquet(filename, index=False, engine='fastparquet')
                i += 1

        except Exception as e:
            print(f"Error in Ingestion Process: {e}")



