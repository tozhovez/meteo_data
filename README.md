# meteo_data
  load_wind_data  update_wind_data avg

  Instructions for Downloading and Installing:
  System Requirements:
    Ubuntu 20.04, python 8.5, docker, ....

  Clone:
    https://github.com/tozhovez/meteo-data.git

  in meteo-data directiory:
  Run:

    make install-requirements

    make run-infra

    make create-database (postgres:12)
      connect to DB postgres://docker:docker@localhost:5333/meteo_data


    make load-wind-data
      extracting and inserting data from meteo-data/archive.zip file


    make update-wind-data
    service run every 300 sec and check directory
        ${HOME}/meteo-data-storage/data
    so 1) change permission ${HOME}/meteo-data-storage/data
        sudo chmod (775 or 777) -R ${HOME}/meteo-data-storage
        copy csv files to ${HOME}/meteo-data-storage/data upsert DB


    make query






