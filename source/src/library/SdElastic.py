from loguru import logger
from elasticsearch.helpers import scan, bulk
from elasticsearch import Elasticsearch

from src.config import SdConfig


class SdElasticConnect:
    def __init__(self, config: SdConfig):
        self.logger = logger
        self.config: SdConfig = config
        self.connectEs = None
        self.configEs = None
    
    def connect(self):
        self.logger.info(f"Attempting to connect to Elasticsearch configuration: {self.config.ES_URL}")
        if self.config:
            try:
                self.connectEs = Elasticsearch(
                    hosts=self.config.ES_URL,
                    http_auth=(self.config.ES_USERNAME, self.config.ES_PASSWORD),
                    timeout=self.config.ES_TIMEOUT
                )

                if self.connectEs.ping():
                    self.logger.success(f"Successfully connected to Elasticsearch: {self.config.ES_URL}")
                else:
                    raise ConnectionError("Failed to connect to Elasticsearch")
            
            except Exception as e:
                self.logger.error(f"Failed to connect to Elasticsearch: {e}")
                raise
    
    
    @staticmethod
    def _buildQueryMatchAll():
        return {"query": {"match_all": {}}}
    

    def searchAll(self):
        self.logger.debug(f"Starting search operation on index: {self.config.ES_INDEX_NAME}")        
        indexName = self.config.ES_INDEX_NAME
        query = self._buildQueryMatchAll()

        try:
            self.logger.debug("Executing Elasticsearch scan query...")
            result = scan(
                self.connectEs, 
                index=indexName, 
                query=query,
                size=1000,
                scroll='5m',
                preserve_order=True,
                request_timeout=self.config.ES_TIMEOUT,
                headers={"Content-Type": "application/json"})
            self.logger.success(f"Search completed successfully.")
            return result
        except Exception as e:
            self.logger.error(f"Error while searching data in index '{indexName}': {e}", exc_info=True)
            raise
    
    def _buildQuery(self, 
                    field: str,
                    gte: int | None = None, 
                    lte: int | None = None, 
                    format: str | None = None,
                    sort_order: str | None = None) -> dict:
        
        self.logger.debug(f"Building query for field '{field}' with parameters: "
                     f"gte={gte}, lte={lte}, sort_order={sort_order}")
        
        range_params = {}
        
        if gte is not None:
            range_params["gte"] = gte
        
        if lte is not None:
            range_params["lte"] = lte
            
        if format is not None:
            range_params["format"] = format
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                field: range_params
                            }
                        }
                    ]
                }
            }
        }
        
        if sort_order is not None:
            query["sort"] = [{field: {"order": sort_order}}]
        
        self.logger.debug(f"Constructed query: {query}")
        
        return query

    def searchQuery(self, gte: int, lte: int):
        indexName = self.config.ES_INDEX_NAME
        query = self._buildQuery(
            self.config.ES_FIELD, gte, lte, 
            self.config.ISO_FORMAT, 
            self.config.SORT_ORDER)

        try:
            self.logger.debug("Executing Elasticsearch scan query...")
            result = scan(
                self.connectEs, 
                index=indexName,
                query=query, 
                size=1000,
                scroll='5m',
                preserve_order=True,
                request_timeout=self.config.ES_TIMEOUT,
                headers={"Content-Type": "application/json"})
            self.logger.success(f"Search completed successfully.")
            return result
        except Exception as e:
            self.logger.error(f"Error while searching data in index '{indexName}': {e}", exc_info=True)
            raise

    def bulkIndex(self, chunk: list, actions: list):
        self.logger.debug(f"Preparing {len(chunk)} documents for bulk indexing.")
        
        try:
            success, failed = bulk(self.connectEs, actions, stats_only=True)
            self.logger.success(f"Successfully indexed {len(chunk)}")
            return success, failed
        except Exception as e:
            self.logger.error(f"Bulk indexing error: {str(e)}")
            raise
        #     return 0, len(chunk)
        # finally:
        #     self.connectEs.close()

    def close(self):
        self.logger.info(f"Close connection Elasticsearch: {self.config.ES_URL}")
        self.connectEs.close()