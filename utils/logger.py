import logging
from typing import Optional

class Logger:
    """Logs info, warning and error messages"""
    def __init__(self, 
                 name: Optional[str] = __name__) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        s_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(
                        "./logs/logs.log", "w")
        
        fmt = logging.Formatter(
            "%(name)s:%(levelname)s - %(message)s")
        
        s_handler.setFormatter(fmt)
        f_handler.setFormatter(fmt)

        s_handler.setLevel(logging.INFO)
        f_handler.setLevel(logging.INFO)

        self.logger.addHandler(s_handler)
        self.logger.addHandler(f_handler)

    def info(self, message: str) -> None:
        """Logs an info message"""
        self.logger.info(message)

    def warn(self, message: str) -> None:
        """Logs a warning message"""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Logs an error message"""
        self.logger.error(message, exc_info=True)