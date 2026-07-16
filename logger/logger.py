import logging

class Logger:
    def __init__(self, name: str, level: int = logging.DEBUG) -> None:
        self.logger = logging.getLogger(name)

        self.logger.propagate = False 
        
        if not self.logger.handlers:
            self.logger.setLevel(level)
            
            ch = logging.StreamHandler()
            ch.setLevel(level)
            
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            
            self.logger.addHandler(ch)

    def debug(self, message: str):
        self.logger.debug(message)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def critical(self, message: str):
        self.logger.critical(message)
