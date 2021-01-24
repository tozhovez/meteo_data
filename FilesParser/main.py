#!/usr/bin/env python3
import asyncio
import time
import datetime
from datetime import datetime, timedelta
from tornado import gen, httpclient, ioloop, queues
from tornado.web import HTTPError
from itertools import groupby
import operator
from common.files_lib import load_config_from_yaml, async_write_file
import pathlib
import json

SERVICE_CONF = pathlib.Path(__file__).parent / "config.yml"
CONF = load_config_from_yaml(SERVICE_CONF)

print(CONF)


def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

def rounded_to_the_last_30th_minute_epoch(dt, delta):
    return dt - (dt - datetime.min) % delta


def calculate_dates(start, stop):

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    stop_date = datetime.strptime(stop, '%Y-%m-%d %H:%M:%S')
    print(start_date, stop_date)
    start_date = ceil_dt(start_date, timedelta(minutes=60))

    while start_date <= stop_date:
        if start_date.hour in (0, 6, 12, 18):
            
            yield start_date
        start_date += timedelta(hours=1)


def create_urls(dates, path, num_zzz):

    for date in dates:
        d = date.strftime('%Y%m%d/%H')
        h = date.strftime('%H')
        yield f"{path}/gfs.{d}/gfs.t{h}z.pgrb2.0p25.f{num_zzz}.idx"


async def get_data_from_url(url=None):
    response = None
    try:
        response = await httpclient.AsyncHTTPClient().fetch(url)
        html = response.body.decode(errors="ignore")
        if html:
            return str(html)
    except httpclient.HTTPClientError as e:
        pass
       
    except HTTPError as e:
        pass
        


def parser(data):
    for line in data.split('\n'):
        d = [i for i in line.split(':') if i]
        if d:
            yield d

def data_dict(data):
    for d in data:
        if d[3] and d[4] and ("mb" in d[4]):
            #print("LINE", d[0], d[1], d[2], d[3], d[4])
            yield {"field": str(d[3]), "Height": str(d[4])}



async def parser_data(data, field, height):
    dataset = list(parser(data))
    ddata = list(sorted(dataset, key=operator.itemgetter(3)))
    dict_data = list(data_dict(ddata))
    numf, numh = 0, 0
    for dd in dict_data:
        #print(dd["field"], dd["Height"], field, height , dd["Height"].strip("mb"))
        if str(dd["field"])==str(field):
           numf += 1
        if str(height) == str(dd["Height"]):
           numh += 1
    print(numf, numh)
    return numf, numh






async def get_data(urls, storage, field, height):
    dl = []
    for url in urls:
        print(url)
        try:
            data = str(await get_data_from_url(url))
            if data:
                dl.append(await parser_data(str(data), field, height))


                await async_write_file(
                    data_object=data,
                    file_name=pathlib.Path(storage)/pathlib.Path(url).name
                    )

        except Exception as e:
            pass
    return dl


async def main():
    #search parameters
    data_storage = pathlib.Path(__file__).parent/ CONF['DATA_STORAGE']
    if not pathlib.Path(data_storage).is_dir():
        pathlib.Path(data_storage).mkdir(parents=True)

    dates = list(calculate_dates(
        CONF['INPUT_PARAMS']['START_DATE'], CONF['INPUT_PARAMS']['STOP_DATE']))
    print(f"Number Datas {len(dates)}")
    urls = list(create_urls(dates, CONF['FILES_HOST'], str(CONF['INPUT_PARAMS']['ZZZ'])))


    data = await get_data(urls, data_storage, CONF['INPUT_PARAMS']['FIELD'], CONF['INPUT_PARAMS']['HEIGHT'])
    print(data)
    print("URL's to download")
    for url in urls:
        print(url)



if __name__ == "__main__":
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(main)





