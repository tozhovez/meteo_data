import asyncio
import asyncpg
from injector import inject, singleton
from logging import Logger

#Exception
class DeadlockDetectedException(Exception):
    """Errors  Deadlock Detected"""
    pass


@singleton
class AsyncPostgresClient:
    @inject
    def __init__(self,
                 address: str,
                 logger: Logger):
        self.address = address
        self.logger = logger
        self.connection = False

    async def connect(self):
        if self.connection is False:
            self.connection = True
        connection = await asyncpg.connect(dsn=self.address)
        settings = await connection.get_settings()
        self.logger.info(f"AsyncPostgresClient connected to: {self.address} {settings}")
        await connection.close()



    def __str__(self):
        return f"AsyncPostgresClient: {self.address}"

    async def close(self):
        if self.connection is True:
            self.connection = False
            self.logger.info(f"AsyncPostgresClient disconnected from: {self.address} ")

    async def select(self, query):
        connection = await asyncpg.connect(dsn=self.address)
        result = await connection.fetch(query)
        await connection.close()
        return [dict(res) for res in result] if result else None


    async def select_record(self, query):
        connection = await asyncpg.connect(dsn=self.address)
        result = await connection.fetch(query)
        await connection.close()
        return result , get_server_pid( )

    async def select_dict(self, query):
       
        connection = await asyncpg.connect(dsn=self.address)
        result = await connection.fetch(query)
        await connection.close()
        return dict(tuple(res) for res in result) if result else None


    async def select_one_value(self, query):
        connection = await asyncpg.connect(dsn=self.address)
        result = await connection.fetch(query)
        await connection.close()
        return result[0][0] if result else None


    async def execute(self, query):
        connection = await asyncpg.connect(dsn=self.address)
        result = None
        try:
            async with connection.transaction():
                result = await connection.execute(query)

        except Exception as ex:
            print(ex)
            #self.logger.info(f"AsyncPostgres Exception {ex}")
            raise DeadlockDetectedException("deadlock detected")
        await connection.close()
        if "INSERT" not in result:
            print(result.encode("utf-8"))
            raise Exception(result)
        return result


async def main():
    address = "postgres://admin:admin@localhost/games"
    client = AsyncPostgresClient(address)
    query = "select * from rus_nouns where noun like '%абаз%'"
    result = await client.select(query)
    #print(result)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
