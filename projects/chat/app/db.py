from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

from .config import settings


_pool: ConnectionPool | None = None


def _configure(conn: psycopg.Connection) -> None:
    register_vector(conn)


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            settings.chat_db_dsn,
            min_size=1,
            max_size=8,
            kwargs={"autocommit": True},
            configure=_configure,
        )
    return _pool


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    with get_pool().connection() as conn:
        yield conn
