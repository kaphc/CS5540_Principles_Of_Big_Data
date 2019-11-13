import logging


class LoggerFile:
    @staticmethod
    def set_logger_file(log_name, log_file_name):
        logger = logging.getLogger(log_name)
        logging.getLogger().setLevel(logging.INFO)

        file_log_handler = logging.FileHandler(log_file_name)
        logger.addHandler(file_log_handler)

        stderr_log_handler = logging.StreamHandler()
        logger.addHandler(stderr_log_handler)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - \n%(message)s')
        file_log_handler.setFormatter(formatter)
        stderr_log_handler.setFormatter(formatter)
        return logger
