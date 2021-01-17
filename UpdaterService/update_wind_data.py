import sys
import os
import asyncio
from logging import Logger
from datetime import datetime
from injector import singleton, inject
from jobs.job_manager import JobManager
from core.config_service import ConfigService
from core.asyncio_postgres_client import AsyncPostgresClient
from services.data_updater import DataUpdaterService


@singleton
class UpdateWindData:
    @inject
    def __init__(self,
                logger: Logger,
                job_manager: JobManager,
                pg_client: AsyncPostgresClient,
                config_service: ConfigService,
                data_updater_service: DataUpdaterService
                ):
        self.logger = logger
        self.pg_client = pg_client
        self.job_manager = job_manager
        self.config_service = config_service
        self.data_updater_service = data_updater_service
        self.service_start_time = None
        self.job_interval = None
        self.service_status = False


    async def status(self):
        if self.service_status:
            pass
        return self.service_status


    async def kill(self):
        """
        Graceful exit
        """
        if self.job_manager:
            self.job_manager.close()
        if self.pg_client:
            await self.pg_client.close()
        if self.data_updater_service:
            await self.data_updater_service.close()


    async def _on_start_data_updater(self):
        try:
            self.logger.info(f"WindDataUpdater Job starting {datetime.utcnow()}")
            if await self.data_updater_service.updater():
                self.logger.info(f"WindDataUpdater Job done {datetime.utcnow()}")
            else:
                self.logger.info(f"WindDataUpdater Job FAILED {datetime.utcnow()}")
        except Exception as ex:
            self.logger.error(
                f"Unexpected error {ex}",
                exc_info=True
                )
            await self.kill()
        finally:
            self.logger.info('Job done')


    async def run(self):
        self.service_start_time = datetime.utcnow()
        self.logger.info(
                f"UpdateWindData service has started {self.service_start_time}"
                )
        # try:
        #     if not self.pg_client:
        #     await self.pg_client.connect()
        # except Exception as ex:
        #     self.logger.info('Cannot connect to postgres', exc_info=True)
        #     exit(1)

        # start background jobs
        self.job_interval = self.config_service.conf['JOB_INTERVAL']
        self.job_manager.add_interval_job(
                self._on_start_data_updater,
                self.job_interval
                )
        self.logger.info(
                f"UpdateWindData service job interval {self.job_interval}"
                )
        try:
            if await self.data_updater_service.updater():
                self.logger.info(f"WindDataUpdater Job done {datetime.utcnow()}")
            else:
                self.logger.info(f"WindDataUpdater Job FAILED {datetime.utcnow()}")
        except(KeyboardInterrupt, SystemExit):
            pass
        try:
            self.job_manager.start()
        except(KeyboardInterrupt, SystemExit):
            pass


