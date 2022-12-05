import os
import sys
import logging
from inspect import currentframe, getframeinfo

# FORMATTER = '%(levelname)-8s [%(asctime)s] %(message)s'
FORMATTER = '{time:HH:mm:ss} | {level} | {message}'
LOG4J = 'log4j.properties'


def get_spark_logger(logger_name, spark, log4j_properties_file_path=LOG4J):
    """
    Получить объект spark логера, для записи логов в АС Журналирование.

    :param logger_name: имя логера, которое будет выводится в каждом сообщении.
    :param spark: текущий spark context.
    :param log4j_properties_file_path: путь до файла с настройками логеров.

    :return: Spark logger
    """
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.Logger.getLogger(logger_name)
    propertyConf = log4j.PropertyConfigurator

    if os.path.exists(log4j_properties_file_path):
        propertyConf.configure(log4j_properties_file_path)
    else:
        propertyConf.configure(os.path.join('.', 'include', log4j_properties_file_path))

    return logger


def get_logger(formatter=FORMATTER, level=logging.INFO, stream=sys.stdout):
    """
    Получить стандартный python logger.

    :param formatter: Формат логирования.
        По умолчанию: '%(levelname)-8s [%(asctime)s] (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s'
    :param level: Уровень логирования.
        По умолчанию: INFO
    :param stream: Поток вывода
        По умолчанию: stdout

    :return: Python logger
    """
    from loguru import logger as async_logger

    async_logger.remove()
    async_logger.add(stream, format=formatter, level=level)
    return async_logger


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ML360Logger(metaclass=Singleton):
    """
    Логер, который может работать со spark

    Функции:
        activate_spark_logger - активация spark логера.

        deactivate_spark_logger - деактивация spark логера.

        info - Логирует с level 'INFO', может принимать несколько аргументов.

        warning - Логирует с level 'WARNING', может принимать несколько аргументов.

        error - Логирует с level 'ERROR', может принимать несколько аргументов.

        debug - Логирует с level 'DEBUG', может принимать несколько аргументов.

        critical - Логирует с level 'CRITICAL, может принимать несколько аргументов'.

    Пример использования:
        logger = ML360Logger()
        logger.info('test')
        logger.info('test1', 'test2', 'test3')
    """
    spark_logger = None
    python_logger = None

    def __init__(self, formatter=FORMATTER, level=logging.INFO, stream=sys.stdout):
        """
        :param formatter: Формат логирования.
            По умолчанию: '%(levelname)-8s [%(asctime)s] (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s'
        :param level: Уровень логирования.
            По умолчанию: INFO
        :param stream: Поток вывода
            По умолчанию: stdout
        """
        self.python_logger = get_logger(formatter, level, stream)

    def activate_spark_logger(self, spark_context, logger_name=None, log4j=LOG4J):
        """
        Активация spark логера

        :param spark_context: текущий spark context
        :param logger_name: имя логера
        :param log4j:
        """
        if logger_name is None:
            logger_name = "LOGGER"
        self.spark_logger = get_spark_logger(logger_name, spark_context, log4j_properties_file_path=log4j)

        if self.spark_logger is not None:
            self.info('SPARK LOGGER ACTIVATED')
        else:
            self.warning('UNABLE TO ACTIVATE SPARK LOGGER')

    def deactivate_spark_logger(self):
        """
        Деактиваця spark логера
        """
        self.spark_logger = None
        self.debug('Spark logger successfully deactivated')

    @staticmethod
    def _format_message(messages: list, frame_info):
        """
        Генератор форматирования сообщений, возвращает строку '(filename).function(lineno) - message' .

        :param messages: Сообщения для форматирования.
        :param frame_info: Информация о том, откуда была вызвана функция.
        """
        for msg in messages:
            yield '({0}).{1}({2}) - {3}'.format(os.path.basename(frame_info.filename),
                                                str(frame_info.function),
                                                str(frame_info.lineno),
                                                str(msg))

    def info(self, message: str, *args):
        """
        Логирует с level 'INFO', может принимать несколько аргументов.

        :param message: message to log
        """
        frame_info = getframeinfo(currentframe().f_back)
        log_msgs = self._format_message([message, *args], frame_info)

        for msg in log_msgs:
            if self.spark_logger is not None:
                self.spark_logger.info(msg)
            self.python_logger.info(msg)

    def warning(self, message: str, *args):
        """
        Логирует с level 'WARNING', может принимать несколько аргументов.

        :param message: message to log
        """
        frame_info = getframeinfo(currentframe().f_back)
        log_msgs = self._format_message([message, *args], frame_info)

        for msg in log_msgs:
            if self.spark_logger is not None:
                self.spark_logger.warn(msg)
            self.python_logger.warning(msg)

    def error(self, message: str, *args):
        """
        Логирует с level 'ERROR', может принимать несколько аргументов.

        :param message: message to log
        """
        frame_info = getframeinfo(currentframe().f_back)
        log_msgs = self._format_message([message, *args], frame_info)

        for msg in log_msgs:
            if self.spark_logger is not None:
                self.spark_logger.error(msg)
            self.python_logger.error(msg)

    def debug(self, message: str, *args):
        """
        Логирует с level 'DEBUG', может принимать несколько аргументов.

        :param message: message to log
        """
        frame_info = getframeinfo(currentframe().f_back)
        log_msgs = self._format_message([message, *args], frame_info)

        for msg in log_msgs:
            if self.spark_logger is not None:
                self.spark_logger.debug(msg)
            self.python_logger.debug(msg)

    def critical(self, message: str, *args):
        """
        Логирует с level 'CRITICAL', может принимать несколько аргументов.
        Spark logger doesn't support "critical", therefore, the message will be processed with severity 'WARNING'

        :param message: message to log
        """
        frame_info = getframeinfo(currentframe().f_back)
        log_msgs = self._format_message([message, *args], frame_info)

        for msg in log_msgs:
            if self.spark_logger is not None:
                self.spark_logger.warn(msg)
            self.python_logger.critical(msg)
