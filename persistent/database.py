import sqlite3
from datetime import datetime
import copy
import re
import logging

import shortuuid
import jsonpickle
from cachetools import LRUCache

from .errors import NotFoundError


logger = logging.getLogger(__name__)
connection = None
objects = None


def _log_sql(sql):
    logger.debug(sql.strip())


def connect(db_path=':memory:',
            debug=False,
            cache_size=1000,
            use_WAL=True):

    global connection
    connection = sqlite3.connect(db_path)

    if debug:
        connection.set_trace_callback(_log_sql)

    if use_WAL:
        connection.execute("PRAGMA journal_type = WAL")

    connection.executescript("""
PRAGMA case_sensitive_like = ON;
CREATE TABLE IF NOT EXISTS objects (json JSON NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS id_index ON objects (json_extract(json, '$.id'));
CREATE INDEX IF NOT EXISTS type_index ON objects (json_extract(json, '$.py/object'));
""")

    global objects
    objects = LRUCache(maxsize=cache_size,
                       missing=get)


def unpickle(text):
    obj = jsonpickle.decode(text)

    # Convert references back to loaded objects.

    refs = obj.__class__.references
    try:
        refs = list(refs)
        for attr in refs:
            ref_id = getattr(obj, attr, None)
            if type(ref_id) is str:
                setattr(obj, attr, objects[ref_id])
    except TypeError as err:
        pass

    return obj


def get(object_id):
    sql = "SELECT json FROM objects WHERE json_extract(json, '$.id')=?"

    row = connection.execute(sql, (object_id,)).fetchone()
    if not row:
        raise NotFoundError('object not found: %s' % object_id)

    obj = unpickle(row[0])
    obj.mark_clean()
    return obj


def index_name(key_paths):
    return '%s__idx' % re.subn(r'[./]', '_', '__'.join(key_paths))[0]


def add_index(key_paths,
              unique=False,
              global_scope=False):

    # Global scope means all objects regardless of their class type.

    if not global_scope:
        index_parts = ["json_extract(json, '$.py/object')"]

    index_parts.extend(["json_extract(json, '$.%s')" % key_path
        for key_path in key_paths])

    name = index_name(key_paths)

    sql = "CREATE %s INDEX IF NOT EXISTS '%s' ON objects (%s)" % (
        'UNIQUE' if unique else '',
        name,
        ', '.join(index_parts)
    )

    connection.execute(sql)

    return name


def transaction():
    """ Use this as a context manager to save/delete a
    bunch of persistent objects in a single transaction """

    return connection
