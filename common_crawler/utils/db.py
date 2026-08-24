import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from common_crawler.constants.enums import APP_ENV, DATABASE_URL

logger = logging.getLogger(__name__)

# The DB is CockroachDB but connections may be configured with a `postgresql`
# URL. CockroachDB >= 25 reports `pg_catalog.version()` without the word
# "PostgreSQL", which breaks the plain PostgreSQL dialect's version parser.
# Route the connection through the CockroachDB dialect instead. We swap only
# the scheme string (not via make_url) so credentials containing special
# characters survive untouched.


def _normalize_url(database_url: str) -> str:
    if database_url.startswith("postgresql+psycopg2://"):
        return "cockroachdb+psycopg2://" + database_url[len("postgresql+psycopg2://"):]
    if database_url.startswith("postgresql://"):
        return "cockroachdb://" + database_url[len("postgresql://"):]
    return database_url


def build_engine(settings: dict) -> Engine:
    database_url = settings.get(DATABASE_URL)

    env = os.getenv(APP_ENV)
    if env == "dev":
        database_url = os.getenv(DATABASE_URL) or database_url

    if not database_url:
        raise ValueError(f"{DATABASE_URL} is not configured")

    database_url = _normalize_url(database_url)

    return create_engine(database_url)
