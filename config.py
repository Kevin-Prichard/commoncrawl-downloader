import os

from box import Box
from clickhouse_sqlalchemy import engines
from sqlalchemy import Index

env = Box(os.environ)
CH_CONNECT_URI = (f'clickhouse+native://{env.CH_USER}:{env.CH_PASSWORD}'
                  f'@{env.CH_HOST}:{env.CH_PORT}/{env.CH_DB_NAME}')
del env

CACHE_REQUESTS = True

# URLs of Common Crawl servers
CC_INDEX_HOSTNAME = 'index.commoncrawl.org'
CC_DATA_HOSTNAME = 'data.commoncrawl.org'

# Clickhouse-Sqlalchemy-specific CommonCrawl table args
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
