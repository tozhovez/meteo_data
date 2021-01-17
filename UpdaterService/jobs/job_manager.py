import asyncio
from injector import inject, singleton
from apscheduler.schedulers.asyncio import AsyncIOScheduler


@singleton
class JobManager:
    @inject
    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    def add_interval_job(self, callback, interval: float):
        self.scheduler.add_job(callback, 'interval', seconds=interval)


    def modify_interval_job(self, callback, interval: float):
        self.scheduler.reschedule_job(callback, 'interval', seconds=interval)

    def run_once(self, callback, args: list):
        job = self.scheduler.add_job(callback, 'date', args=args)
        return job

    def start(self):
        if self.scheduler is None:
            self.scheduler = AsyncIOScheduler()
        self.scheduler.start()

    def close(self):
        if self.scheduler:
            self.scheduler.shutdown()
            self.scheduler = None

