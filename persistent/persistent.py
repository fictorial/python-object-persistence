import copy
from datetime import datetime
import sqlite3
import re

import shortuuid
import jsonpickle

from .errors import UniquenessError, NotFoundError
from . import database


class Persistent:
    """ See README """

    def __init__(self):
        self.id = shortuuid.uuid()


    def __setattr__(self, key, value):
        self.__dict__[key] = value
        self.mark_dirty()


    def mark_dirty(self):
        self.__dict__['_dirty'] = True    # avoid setattr


    def mark_clean(self):
        try:
            del self._dirty
        except AttributeError:
            pass


    @property
    def is_dirty(self):
        return hasattr(self, '_dirty')


    @property
    def is_new(self):
        return not hasattr(self, 'created_at')


    references = None


    def _with_references(self):
        """
        Return a copy of this object with references to
        Persistent objects replaced with their `id`.
        """

        try:
            refs = list(self.references)
        except TypeError:
            return self

        obj = copy.copy(self)

        for attr in refs:
            referenced = getattr(self, attr, None)
            if referenced:
                if isinstance(referenced, Persistent):
                    if referenced.is_new:
                        referenced.save(False)
                    setattr(obj, attr, referenced.id)

        return obj


    def save(self, use_transaction=True):
        if not self.is_dirty:
            return self

        if use_transaction:
            with database.connection:
                return self._save()

        return self._save()


    def _save(self):
        to_save = self._with_references()

        now = datetime.utcnow()

        try:
            if self.is_new:
                if self != to_save:
                    to_save.created_at = now

                to_save.mark_clean()

                sql = "INSERT INTO objects VALUES (json(?))"
                database.connection.execute(sql, (jsonpickle.encode(to_save),))
            else:
                if self != to_save:
                    to_save.updated_at = now

                to_save.mark_clean()

                sql = "UPDATE objects SET json=json(?) WHERE json_extract(json, '$.id')=?"
                database.connection.execute(sql, (
                    jsonpickle.encode(to_save),
                    to_save.id))

            if self.is_new:
                self.created_at = now
            else:
                self.updated_at = now

            self.mark_clean()

            return to_save

        except sqlite3.DatabaseError as err:
            self.mark_dirty()

            err = str(err)
            if 'UNIQUE' in err:
                match = re.match(r"'([^']+)'", err)
                index_name = match.groups()[0] if match else ''
                raise UniquenessError(index_name)

            raise


    def delete(self, use_transaction=True):
        sql = "DELETE FROM objects WHERE json_extract(json, '$.id')=?"

        if use_transaction:
            with database.connection:
                database.connection.execute(sql, (self.id,))
        else:
            database.connection.execute(sql, (self.id,))
