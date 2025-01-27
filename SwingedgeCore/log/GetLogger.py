import logging
import os

class Logger:
    def __init__(self,logging_file='application'):
          self.logging_file = logging_file
          
    def get_logger(self):
            log_directory = "logging"
            os.makedirs(log_directory, exist_ok=True)
                
            logging.basicConfig(
                filename=os.path.join(log_directory, f"{self.logging_file}.log"), 
                level=logging.DEBUG, 
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
            return logging.getLogger()