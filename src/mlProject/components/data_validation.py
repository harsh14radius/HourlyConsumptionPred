from src.mlProject import logger
from src.mlProject.entity.config_entity import DataValidationConfig


class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def initiateDataValidation(self):
        try:
            pass

        except Exception as e:
            print(f"Error in Validation Process: {e}")
