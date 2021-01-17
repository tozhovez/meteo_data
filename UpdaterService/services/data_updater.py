import pprint
import asyncio
import pathlib
import json
from datetime import datetime
from logging import Logger
from codetiming import Timer
from injector import singleton, inject
from core.config_service import ConfigService
from core.asyncio_postgres_client import AsyncPostgresClient, DeadlockDetectedException
from common.files_lib import file_read_csv, async_file_read_csv
from services.postgres_queries import QUERIES
from collections import Counter


#Exception
class DataErrorsException(Exception):
    """Errors Input data """
    pass

class DataFileErrorsException(Exception):
    """Errors data file """
    pass


@singleton
class DataUpdaterService:
    """DataUpdaterService"""
    @inject
    def __init__(
        self,
        logger: Logger,
        pg_client: AsyncPostgresClient,
        config_service: ConfigService
        ):
        self.logger = logger
        self.config_service = config_service
        self.pg_client = pg_client
        self.data_storage_dir = self.config_service.conf['DATA_STORAGE']
        self.archive_storage_dir =  self.config_service.conf['ARCHIVE_STORAGE']
        self.max_lines = self.config_service.conf['MAX_LINES']
        self.table_data = self.config_service.conf['TABLE_DATA']
        self.table_city = self.config_service.conf['TABLE_CITY']
        self.cities = {}
        self.work_queue = asyncio.Queue()

    async def close(self):
        self.logger.info("UpdaterData service stop")
        pass


    async def updater(self):
        self.logger.info(
            f"UpdaterData settings data_storage_dir:{self.data_storage_dir},archive_storage_dir:{self.archive_storage_dir}, max_lines:{self.max_lines}"
            )
        try:
            csvfiles = await self.check_data_dir()
            if csvfiles and self.data_storage_dir.joinpath(
                    "city_attributes.csv") in csvfiles:
                csvfiles = await self.city_attributes_data_updater(
                    self.data_storage_dir.joinpath("city_attributes.csv")
                    )
            if csvfiles:
                self.cities.update(await self.pg_client.select_dict(
                    QUERIES['query_get_cities']()))
                self.logger.info(f"UpdaterData cities in DB {self.cities}")
                return await self.get_data(csvfiles)
            else:
               self.logger.info(
                f"{self.data_storage_dir} is empty - no data to update"
                )
               return True

        except Exception as ex:

            raise ex

        return False


    async def check_data_dir(self):
        try:
            return list(pathlib.Path(self.data_storage_dir).iterdir())
        except Exception as ex:
            raise ex

    async def worker(self, name):
        while not self.work_queue.empty():
            filename = await self.work_queue.get()
            self.logger.info(f"Task {name} getting reading: {filename.name}")
            try:
                result = await self.data_uploader(filename)
            except(DataFileErrorsException) as ex:
                self.logger.exception(ex)
            except Exception as ex:
                self.logger.exception(ex)
                raise ex
            self.work_queue.task_done()
        return result


    async def get_data(self, csvfiles):
        try:
            for filename in csvfiles:
                await self.work_queue.put((filename))

            result = await asyncio.gather(*(asyncio.create_task(
                self.worker(f"TASK_{t+1}")) for t,_ in enumerate(csvfiles)),
                                          return_exceptions=True)
            return all(result)
        except Exception as ex:
            raise ex
        return True


    async def data_uploader(self, filename):
        self.logger.info(f"UpdaterData Job processing data from {filename.name}")
        try:
            datatype, valuetype = self._datatype_(filename.name)
            if await self.data_formatting(
                data=file_read_csv(filename),
                datatype=datatype,
                valuetype=valuetype,
                filename=filename
                ):
                    pathlib.Path(filename).rename(
                        self.archive_storage_dir.joinpath(filename.name))
                    self.logger.info(
                        f"UpdaterData Job move {filename.name} to archive_storage")
                    return True
        except(DataFileErrorsException) as ex:
            self.logger.exception(ex)
        except Exception as ex:
            raise ex
        return False


    def _datatype_(self, filename):
        if "wind_direction" in filename:
            return "wind_direction", "numeric"
        elif "wind_speed" in filename:
            return "wind_speed", "numeric"
        elif "humidity" in filename:
            return "humidity", "numeric"
        elif "pressure" in filename:
            return "pressure", "numeric"
        elif "temperature" in filename:
            return "temperature", "numeric"
        elif "weather_description" in filename:
            return "weather_description", "text"
        else:
            raise DataFileErrorsException(f"Unknow data filename {filename}")


    async def data_formatting(self, data, datatype, valuetype, filename):
        j = self.max_lines
        values = []
        for i, row in enumerate(data):
            try:
                values.extend(list(self.transform(row, valuetype, filename)))
                if i>=j :
                    if values:
                        await self.pg_client.execute(QUERIES["query_upsert_to_wind_data"]({
                                    "table": self.table_data,
                                    "datatype": datatype,
                                    "values": ",".join(values)
                                }))
                    j+=self.max_lines
                    values = []
            except(DeadlockDetectedException) as ex:
                self.logger.error(ex)
            except(DataErrorsException) as ex:
                self.logger.exception(ex)
            except Exception as exe:
                raise exe
        if values:
            try:
                await self.pg_client.execute(QUERIES["query_upsert_to_wind_data"]({
                                "table": self.table_data,
                                "datatype": datatype,
                                "values": ",".join(values)
                            }))
            except(DeadlockDetectedException) as ex:
                self.logger.error(ex)
            except(DataErrorsException) as ex:
                self.logger.exception(ex)
            except Exception as exe:
                raise exe
        return True


    def transform(self, rdata, valtype, filename):
        dt = rdata.pop("datetime")
        if not dt:
            #self.logger.exception(f"error data empty value datetime in {filename} {rdata}")
            raise DataErrorsException(
                f"error data empty value datetime in {filename}")
        row_data = tuple(filter(lambda k: k[1] != '', rdata.items()))
        if not row_data:
            #self.logger.exception(f"error data empty value in {filename} {rdata}")
            raise DataErrorsException(f"error data empty values in {filename}")
        for k, v in row_data:
            yield str(f"('{dt}'::timestamp, '{self.cities[k]}'::integer, '{v}'::{valtype})")
        return


    async def city_attributes_data_updater(self, filename):
        self.logger.info(f"UpdaterData Job processing data from {filename.name}")
        try:
            data = file_read_csv(filename)
            city_data = list(self.queryformating(data, "city_attributes.csv"))
            if city_data:
                querydata = ", ".join(city_data)
                await self.pg_client.execute(
                    QUERIES["query_update_to_city_attributes"](querydata))
                #print(self.archive_storage_dir.joinpath(filename.name))
            pathlib.Path(filename).rename(
                self.archive_storage_dir.joinpath(filename.name))
            self.logger.info(
                f"UpdaterData Job move {filename.name} to archive_storage")
            return await self.check_data_dir()
        except(DataErrorsException) as ex:
            self.logger.exception(ex)
        except Exception as exe:
            raise exe



    def queryformating(self, data, filename):
        for row in data:
            if row["City"] == "" or row["Country"] == "":
                #self.logger.exception(f"error data empty value  in {filename} {row}")
                raise DataErrorsException(f"error data empty in {filename}")
            else:
                lat = str(f"'{row['Latitude']}'::numeric"
                          if row['Latitude'] != ''
                          else f"{json.dumps(None)}::numeric")
                lon = str(f"'{row['Longitude']}'::numeric"
                          if row['Longitude'] != ''
                          else f"{json.dumps(None)}::numeric")
                yield str(f"('{row['City']}'::text, '{row['Country']}'::text, {lat}, {lon})")

