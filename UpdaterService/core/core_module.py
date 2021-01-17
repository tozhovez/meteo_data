
import os
import logging
from logging import Logger
from injector import provider, singleton, Module
from core.logging import get_logger, LoggingOutput, LoggingFormat
from core.asyncio_postgres_client import AsyncPostgresClient
from core.config_service import ConfigService


class CoreModule(Module):
    """config modules"""

    @singleton
    @provider
    def provide_postgres_client(self, config_service: ConfigService, logger: Logger) -> AsyncPostgresClient:
        """innit postgres client"""
        
        postgres_client = AsyncPostgresClient(address=config_service.conf['POSTGRES_DB_URL'], logger=logger)

        return postgres_client


    @singleton
    @provider
    def provide_logger_UpdateWindData(self) -> Logger:
        """innit logger"""
        logger = get_logger(
            logger_name="UpdateWindData",
            logger_version='0.0.1',
            logger_format=LoggingFormat.JSON,
            logger_level=logging.INFO,
            logger_output=LoggingOutput.STDOUT,
        )
        return logger


    
if __name__ == "__main__":
    configer = ConfigService()
    con = CoreModule()


    print(con.provide_postgres_client(configer))
    log = get_logger(logger_name=__name__,
                         logger_version='0.0.0.1',
                         logger_format=LoggingFormat.JSON,
                         logger_level=logging.INFO,
                         logger_output=LoggingOutput.STDOUT)

    log.info("logging: info", extra={'test': __name__})
    log.debug("logging: debug", extra={'test': __name__})
    log.note("logging: note", extra={'test': __name__})
    log.warning("logging: warning", extra={'test': __name__})
    log.error("logging: error", extra={'test': __name__})
    log.critical("logging: critical", extra={'test': __name__})
    log.exception("logging: exception", extra={'test': __name__})
