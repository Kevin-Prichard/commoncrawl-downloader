import os

from box import Box
from clickhouse_sqlalchemy import engines
from enum import Enum

from sqlalchemy import (INTEGER, SMALLINT, BIGINT, FLOAT, TIMESTAMP,
                        String, TEXT, BOOLEAN, JSON, ARRAY,
                        Enum as EnumType)

COL_SMALLINT = SMALLINT
COL_INTEGER = INTEGER
COL_BIGINT = BIGINT
COL_FLOAT = FLOAT
COL_TIMESTAMP = TIMESTAMP
COL_STRING = String
COL_TEXT = TEXT
COL_BOOLEAN = BOOLEAN
COL_JSON = JSON
COL_ARRAY = ARRAY
COL_ENUM = EnumType

CACHE_REQUESTS = True

# URLs of Common Crawl servers
CC_INDEX_HOSTNAME = 'index.commoncrawl.org'
CC_DATA_HOSTNAME = 'data.commoncrawl.org'


class DbType(Enum):
    CLICKHOUSE = 'CLICKHOUSE'
    SQLITE = 'SQLITE'
    POSTGRES = 'POSTGRES'
    MYSQL = 'MYSQL'


""" Per-database type, per-table configurations """
DB_TYPE = DbType.POSTGRES

# Default is nothing special
TABLE_ARGS_Crawl = None
TABLE_DEFAULTS_CdxFirstUrl = None
TABLE_DEFAULTS_WebTextEmbeddings = None
TABLE_DEFAULTS_WarcRecord = None
CONNECT_TIMEOUT=10

env = Box(os.environ)
if DB_TYPE == DbType.POSTGRES:
    # Postgres-Sqlalchemy-specific CommonCrawl table args
    DB_CONNECT_URI = (f'postgresql://{env.DB_USER}:{env.DB_PASSWORD}'
                      f'@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}'
                      f'?connect_timeout={CONNECT_TIMEOUT}')


elif DB_TYPE == DbType.CLICKHOUSE:
    # Clickhouse-Sqlalchemy-specific CommonCrawl table args
    DB_CONNECT_URI = (f'clickhouse+native://{env.DB_USER}:{env.DB_PASSWORD}'
                      f'@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}')
    TABLE_ARGS_Crawl = (engines.ReplacingMergeTree(
        order_by=['id'],
        index_granularity=8192,
    ),)

    TABLE_DEFAULTS_CdxFirstUrl = (engines.ReplacingMergeTree(
        order_by=['crawl_id','cdx_num','tld', 'domain', 'subdomain', 'path'],
        index_granularity=8192,
    ),)

    TABLE_DEFAULTS_WebTextEmbeddings = (engines.ReplacingMergeTree(
        order_by=['url'],
        index_granularity=8192,
    ),)

    TABLE_DEFAULTS_WarcRecord = (
        engines.ReplacingMergeTree(
            order_by=['warc_url'],
            index_granularity=8192,
        ),
        # Index('ix_warc_record_url', 'warc_url', unique=False, clickhouse_type='hash'),
    )

    from clickhouse_sqlalchemy import types
    COL_SMALLINT = types.UInt16
    COL_INTEGER = types.UInt32
    COL_BIGINT = types.UInt64
    COL_FLOAT = types.Float32
    COL_TIMESTAMP = types.DateTime
    COL_STRING = types.String
    COL_TEXT = types.String
    COL_BOOLEAN = types.Boolean
    # COL_JSON = types.JSON
    COL_ARRAY = types.Array
    COL_ENUM = types.Enum

del env
