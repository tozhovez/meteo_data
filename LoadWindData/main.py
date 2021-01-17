
#
import os
import asyncio
import pathlib
import zipfile
import csv

import subprocess
from asyncio_postgres_client import AsyncPostgresClient
import yaml

#Exception
class DataErrorsException(Exception):
    """Errors Input data """
    pass

class DataFileErrorsException(Exception):
    """Errors data file """
    pass


def file_read_csv(filename):
    with open(filename,"r",encoding="utf-8", newline='') as csvfile:
        for row in iter(csv.DictReader(csvfile, restval=None)):
            yield row

def load_config_from_yaml(filename):
    with open(filename, "r", encoding='utf-8') as fd_reader:
        return yaml.full_load(fd_reader)


PROJ_ROOT = pathlib.Path(__file__).parent.parent /"Infra" /"configs" / "config.yml"
SERVICE_CONF = pathlib.Path(__file__).parent.parent / "configs" / "config.yml"

cofigs = load_config_from_yaml(PROJ_ROOT)
pg_client = AsyncPostgresClient(address=cofigs['POSTGRES_DB_URL'])


def query_get_cities():
    query = """
        SELECT  "City", "CityId"
        FROM "city_attributes"
    """
    return query

async def data_files(cofigs=cofigs):
    zip_archive = pathlib.Path(__file__).parent.parent / cofigs['ZIP_INIT_DATA']
    dir_archive =  pathlib.Path(__file__).parent.parent /cofigs['DIR_INIT_DATA']

    if not zip_archive.exists():
        #TODO Load zip datafile
        print("not zip_archive.exists()")
    ziparchive = zipfile.ZipFile(zip_archive)
    ziparchive.extractall(path=dir_archive)
    datafiles = dict(zip(cofigs['DATAFILES'], range(len(cofigs['DATAFILES']))))
    datatypes = dict(zip(range(len(cofigs['DATATYPES'])), cofigs['DATATYPES']))
    data = dict.fromkeys(cofigs['DATATYPES'])
    for fname in list(pathlib.Path(dir_archive).iterdir()):
        if fname.name in datafiles:
            data[datatypes[datafiles[fname.name]]] = {
                "filepath": fname,
                }
    return data

def data_formatting(newdata , data, datatype, cities):
    for row in data:
        for line in list(transform(row, datatype, cities)):
            if (line["datetime"],line["CityId"]) in newdata:
                newdata[(line["datetime"],line["CityId"])].update(line)
            else:
                newdata[(line["datetime"],line["CityId"])] = line



def transform(rdata, datatype, cities):
    dt = rdata.pop("datetime")
    if not dt:
        raise DataErrorsException(
            f"error data empty value datetime")
    row_data = tuple(filter(lambda k: k[1] != '', rdata.items()))
    if not row_data:
        raise DataErrorsException(f"error data empty values")
    for k, v in row_data:
        yield {"datetime": dt, "CityId": cities[k], datatype: v}
    return


def data_reader(dataset, cities):
    #['wind_direction', 'wind_speed', 'humidity', 'pressure', 'temperature', 'weather_description']
    #dataset['wind_direction']['filepath']
    newdata = {}
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset['wind_direction']['filepath']),
        datatype='wind_direction',
        cities=cities
        )
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset['wind_speed']['filepath']),
        datatype='wind_speed',
        cities=cities
        )
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset['humidity']['filepath']),
        datatype='humidity',
        cities=cities
        )
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset['pressure']['filepath']),
        datatype='pressure',
        cities=cities
        )
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset['temperature']['filepath']),
        datatype='temperature',
        cities=cities
        )
    data_formatting(
        newdata=newdata,
        data=file_read_csv(dataset["weather_description"]['filepath']),
        datatype="weather_description",
        cities=cities
        )

    with open('wind_data.csv', 'w', newline='', encoding="utf-8",) as csvfile:
        fieldnames = ["datetime", "CityId", "wind_direction",
                      "wind_speed", "humidity","pressure","temperature","weather_description"]
        writer = csv.DictWriter(csvfile, delimiter=",", fieldnames=fieldnames)
        writer.writeheader()
        for k,v in newdata.items():
            writer.writerow(v)


def inserter(file_name):
    tb_name = "wind_data"
    line = str(f"\copy \"{tb_name}\"(\"datetime\", \"CityId\", \"wind_direction\", \"wind_speed\", \"humidity\", \"pressure\", \"temperature\", \"weather_description\") FROM '{file_name}' delimiter ',' csv header")
    pg_url = str(f"postgres://docker:docker@localhost:5333/meteo_data")
    subprocess.run(["psql", pg_url, "-X", "--quiet", "-c", line])


async def main():
    cities = await pg_client.select_dict(query_get_cities())
    dataset = await data_files()
    data_reader(dataset, cities)
    inserter("wind_data.csv")
    if pathlib.Path("wind_data.csv").exists():
        pathlib.Path("wind_data.csv").unlink()
    return



if __name__ == "__main__":
    #cofigs = load_config_from_yaml(PROJ_ROOT)
    print(cofigs)
    asyncio.get_event_loop().run_until_complete(main())
