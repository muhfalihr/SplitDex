from src.utility.SdValidator import SdValidator


_configini = SdValidator()

_elastic_config = _configini.get_elastic_config()

ES_URL: str = _elastic_config.es_url
ES_USERNAME: str = _elastic_config.es_username
ES_PASSWORD: str = _elastic_config.es_password
ES_TIMEOUT: int = int( _elastic_config.es_timeout )
ES_INDEX_NAME: str = _elastic_config.es_index_name
ES_FIELD: str = _elastic_config.es_field

_engine_config = _configini.get_engine_config()

BATCH_SIZE: int = int( _engine_config.batch_size )
MAX_RETRY_CONNECTION: int = int(_engine_config.max_retry_connection)
FORMAT_DATE: str = _engine_config.format_date

_query_config = _configini.get_query_config()

USED_QUERY = _query_config.used_query
GTE = _query_config.gte
LTE = _query_config.lte
ISO_FORMAT = _query_config.iso_format
SORT_ORDER = _query_config.sort_order