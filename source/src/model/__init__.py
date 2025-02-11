from datetime import datetime
from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from enum import Enum


class MappingData(BaseModel):
    dataId: str = Field(..., title="Data ID")
    indexName: str = Field(..., title="Index Name")
    data: Dict[str, Any] = Field(..., title="Data Result")


class DateFormatter:
    FORMATS = {
        "YYYYmm": "%Y%m",
        "YYYYmmdd": "%Y%m%d",
        "YYYY-mm-dd": "%Y-%m-%d",
        "ddmmYYYY": "%d%m%Y"
    }

    @staticmethod
    def formateDate(date: datetime, fmt: str):
        """Returns a formatted date string based on the selected format."""
        if fmt not in DateFormatter.FORMATS:
            raise ValueError(f"Format '{fmt}' is not supported.")
        return date.strftime(DateFormatter.FORMATS[fmt])
    
    @staticmethod
    def parseDate(date_str: str, fmt: str):
        """Converts a date string into a datetime object."""
        if fmt not in DateFormatter.FORMATS:
            raise ValueError(f"Format '{fmt}' is not supported.")
        return datetime.strptime(date_str, DateFormatter.FORMATS[fmt])


class DateFormats(Enum):
    """Supported date formats."""
    YYYYMM = "YYYYmm"
    YYYYMMDD = "YYYYmmdd"
    YYYY_MM_DD = "YYYY-mm-dd"
    DDMMYYYY = "ddmmYYYY"


class SortOrder(Enum):
    """Valid sort orders."""
    ASC = "asc"
    DESC = "desc"


class IsoFormat(Enum):
    """Supported ISO time formats."""
    EPOCH_MILLIS = "epoch_millis"
    EPOCH_SECOND = "epoch_second"


@dataclass
class ElasticConfig:
    """Elasticsearch configuration parameters."""
    es_url: str
    es_username: str
    es_password: str
    es_timeout: int
    es_index_name: str
    es_field: str


@dataclass
class EngineConfig:
    """Engine configuration parameters."""
    batch_size: int
    max_retry_connection: int
    format_date: DateFormats


@dataclass
class QueryConfig:
    """Query configuration parameters."""
    used_query: bool
    gte: Optional[str] = None
    lte: Optional[str] = None
    iso_format: str = None
    sort_order: str = SortOrder.ASC
