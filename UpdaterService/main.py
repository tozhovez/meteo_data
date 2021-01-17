import os
import sys
import signal
#import socketio
import asyncio
import logging
from sanic import Sanic
from sanic_cors import CORS
from injector import Injector, InstanceProvider, CallableProvider, Module

from core.core_module import CoreModule
from update_wind_data import UpdateWindData

app = None
tsu_service = None


async def main(ioc_container):
    global tsu_service
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    # set a fatal exception handler
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(fatal_exception_handler)

    # start service
    tsu_service = ioc_container.get(UpdateWindData)
    await tsu_service.run()



def fatal_exception_handler(loop, context):
    global tsu_service

    logger = logging.getLogger('UpdateWindData')

    exception = context.get('exception')
    logger.error(
        'Fatal error in UpdateWindData Service, closing program..',
        extra={"exception": str(exception)}
        )
    loop.create_task(tsu_service.kill())
    app.stop()


async def setup_db(app, loop):
    #app.db = await db_setup()
    print("listener setup_db before_server_start")


async def shutdown(signal, loop):
    global tsu_service

    logger = logging.getLogger('UpdateWindData')
    logger.info('bye')

    await tsu_service.kill()


if __name__ == "__main__":
    app = Sanic(name='UpdateWindData', configure_logging=True)
    CORS(app, automatic_options=True)
    ioc_container = Injector(modules=[CoreModule])

    # catch signals
    #app.register_listener(setup_db, "before_server_start")
    app.register_listener(shutdown, 'before_server_stop')
    app.add_task(main(ioc_container))
    app.run(host="0.0.0.0", port=21003, workers=1)
