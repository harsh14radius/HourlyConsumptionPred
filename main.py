from src.mlProject import logger
from src.mlProject.pipeline.stage_01_data_ingestion import DataIngestionTrainingPipeline

logger.info("We are printing the logs here!!!")

STAGE_NAME = "Data Ingestion"

try:
    logger.info(f"-----------stage {STAGE_NAME} started------------")
    obj = DataIngestionTrainingPipeline()
    obj.main()
    logger.info(f"----------------{STAGE_NAME} completed---------------------")
except Exception as e:
    logger.exception(e)
    raise e
