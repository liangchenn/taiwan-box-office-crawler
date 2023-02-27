import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(name)s] %(message)s",
)


class Logger:
    def __init__(self, loggerName, log_level="INFO"):

        # Set up logging
        self.log_name = loggerName
        self.log_level = log_level
        self.logger = logging.getLogger(self.log_name)
        self.logger.setLevel(self.log_level)

        # # Set up logging to file
        # self.logFilename = "./logs/log"
        # self.log_maxBytes = 20
        # self.log_backupCount = 5
        # logging.basicConfig(
        #     level = self.log_level,
        #     format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        #     handlers = [logging.handlers.RotatingFileHandler(filename = self.logFilename,
        #                                                     maxBytes = self.log_maxBytes,
        #                                                     backupCount = self.log_backupCount)]
        # )

        # # Set up logging to console
        # console = logging.StreamHandler(sys.stdout)
        # console.setLevel(self.log_level)
        # formatter = logging.Formatter(
        #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        # )
        # console.setFormatter(formatter)
        # self.logger.addHandler(console)

    def get_logger(self):
        return self.logger
