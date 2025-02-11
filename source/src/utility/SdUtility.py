import os
import time
import configparser

from datetime import datetime, timezone
from functools import reduce
from multiprocessing import cpu_count

from src.model import DateFormatter


def getConfigIni(file_path="config.ini"):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file '{file_path}' not found.")
    
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def argToJson(**kwargs):
    return kwargs


def timestampToDate(timestamp: int, fmt: str):
    dt: datetime = datetime.fromtimestamp(timestamp)
    formattedDt = DateFormatter.formateDate(dt, fmt)
    return formattedDt


def dateToEpoch(dates: str, millis: bool = False):
    if isinstance(dates, str):
        year, month, day = (int(ymd) for ymd in dates.split("-"))
        dt = datetime(year, month, day, tzinfo=timezone.utc)
        
        if millis:
            epoch = int(dt.timestamp() * 1000)
            return epoch
        
        epoch = int(dt.timestamp())
        return epoch

    return None


def validateTimestampQuery(
        gte: str | None, lte: str| None, timestamp: int, 
        gte_threshold: int | None, lte_threshold: int | None):
    if gte and lte:
        return timestamp >= gte_threshold and timestamp <= lte_threshold
    elif gte:
        return timestamp >= gte_threshold
    elif lte:
        return timestamp <= lte_threshold


def templateIndex(index: str, date: str):
    return "{index}-{date}".format(index=index, date=date)


def getNestedValue(data: dict, keys: str):
    try:
        return reduce(
            lambda d, key: d.get(key, None) \
                if isinstance(d, dict) else None, 
                keys.split('.'), data)
    except AttributeError:
        return None


def numProcess():
    return max(1, int( cpu_count() * 0.75 ))