from typing import Dict, List, Any, Generator

from src.model import MappingData
from src.config import SdConfig
from src.library.SdElastic import SdElasticConnect
from src.utility.SdUtility import (
    argToJson, 
    timestampToDate, 
    templateIndex, 
    getNestedValue,
    dateToEpoch,
    validateTimestampQuery)


class SdController:
    def __init__(self):
        self.logger = None
        self.config: SdConfig = None
        self.es: SdElasticConnect = None
        self.gte = None
        self.lte = None
    
    def getData(self):
        if self.config.USED_QUERY == "no":
            datas: Generator[Any, None, None] = self.es.searchAll()
        else:
            if self.config.ISO_FORMAT in ["epoch_millis", "epoch_second"]:
                self.gte = dateToEpoch(self.config.GTE)
                self.lte = dateToEpoch(self.config.LTE)
            else:
                self.gte = self.config.GTE
                self.lte = self.config.LTE

            datas: Generator[Any, None, None] = self.es.searchQuery(self.gte, self.lte)
        return datas

    def mappingData(self, data: Dict[Any, None]):
        id: str = data.get("_id", None)
        source: Dict[Any, None] = data.get("_source", {})
        timestamp: int = getNestedValue(source, self.config.ES_FIELD)
        date: str = timestampToDate(timestamp, self.config.FORMAT_DATE)
        indexName: str = templateIndex(self.config.ES_INDEX_NAME, date)

        data: MappingData = MappingData(**argToJson(
            dataId=id,
            indexName=indexName,
            data=source
        ))

        if self.config.USED_QUERY == "no":
            pass

        else:
            if validateTimestampQuery(
                self.config.GTE, self.config.LTE, timestamp,
                self.gte, self.lte):

                return data.model_dump()
            
            return {}

        return data.model_dump()