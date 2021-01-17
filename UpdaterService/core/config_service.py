
import os
import pathlib
import yaml

from injector import singleton
from yaml import Loader
from common.files_lib import load_config_from_yaml
PROJ_ROOT = pathlib.Path(__file__).parent.parent
SERVICE_CONF = pathlib.Path(__file__).parent.parent / "configs" / "config.yml"

@singleton
class ConfigService:
    """config parameters"""
    def __init__(self):
        self.conf = {}
        if os.getenv('RUNS_IN_DOCKER'):

            self.conf['JOB_INTERVAL'] = os.environ['JOB_INTERVAL']
            self.conf['POSTGRES_DB_URL'] = os.environ['POSTGRES_DB_URL']
            self.conf['HOME_DIR'] = os.environ['HOME_DIR']
            self.conf['DATA_STORAGE'] = os.environ['DATA_STORAGE']
            self.conf['ARCHIVE_STORAGE'] = os.environ['ARCHIVE_STORAGE']

        else:
            self.conf = load_config_from_yaml(SERVICE_CONF) #str(f"{PROJ_ROOT}/configs/config.yml"))
            self.conf['HOME_DIR'] = pathlib.Path.home()
            self.conf['DATA_STORAGE'] = pathlib.Path(self.conf['HOME_DIR']) / self.conf['DATA_STORAGE']
            self.conf['ARCHIVE_STORAGE'] = pathlib.Path(self.conf['HOME_DIR']) / self.conf['ARCHIVE_STORAGE']





if __name__ == "__main__":
    print(f"PROJ_ROOT: {PROJ_ROOT}")
    print(SERVICE_CONF)
    configer = ConfigService()

    print(configer.conf)
    #asyncio.get_event_loop().run_until_complete(main())
