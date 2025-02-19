"""Test configuration that uses SQLite in-memory database"""
import os
from datetime import datetime, timezone
from box import Box

# Create test environment configuration
env = Box({
    'DB_USER': 'test',
    'DB_PASSWORD': 'test',
    'DB_HOST': 'localhost',
    'DB_PORT': '5432',
    'DB_NAME': 'test',
    'CACHE_REQUESTS': True,
})

# Use SQLite in-memory database for testing
DB_CONNECT_URI = 'sqlite:///:memory:'

# Common Crawl configuration
CC_DATA_HOSTNAME = "data.commoncrawl.org"
CC_TIMESTAMP_STRPTIME = "%Y%m%d%H%M%S"

# Database table configuration
TABLE_ARGS_Crawl = {}
TABLE_DEFAULTS_CdxFirstUrl = {}
TABLE_DEFAULTS_WebTextEmbeddings = {}
TABLE_DEFAULTS_WarcRecord = {}

# Column types
COL_SMALLINT = "SMALLINT"
COL_INTEGER = "INTEGER"
COL_BIGINT = "BIGINT"
COL_FLOAT = "FLOAT"
COL_TIMESTAMP = "TIMESTAMP"
COL_STRING = lambda x: f"VARCHAR({x})"
COL_TEXT = "TEXT"
COL_BOOLEAN = "BOOLEAN"
COL_JSON = "JSON"
COL_ARRAY = lambda x: f"ARRAY({x})"
COL_ENUM = lambda x: f"ENUM({x})"

# Backoff configuration
BACKOFF_MAX_TIME = 60
BACKOFF_MAX_TRIES = 5
BACKOFF_EXCEPTIONS = (Exception,)

# Cache configuration
CACHE_REQUESTS = True
