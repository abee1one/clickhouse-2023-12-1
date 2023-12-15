import datetime
import time

import psutil

from infi.clickhouse_orm import Database

from models import CPUStats

db = Database('test')
db.create_table(CPUStats)

while True:
    time.sleep(1)
    timestamp = datetime.datetime.now()
    stats = psutil.cpu_percent(percpu=True)
    db.insert([CPUStats(timestamp=timestamp)])
