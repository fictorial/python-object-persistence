import re

import ujson as json
import keypath

from . import database
from .persistent import Persistent


def _extract(key_path):
    return "json_extract(json, '$.%s')" % key_path


def _qualified_class_name(cls):
    return '%s.%s' % (cls.__module__, cls.__name__)


class Query:
    """
    A query builder for searching for persistent objects.
    """

    def __init__(self, cls=None):
        self._sort = None
        self._regexes = None
        self._limit = 0
        self._skip = 0
        self._where = []
        if cls:
            self._add_condition('py/object', '=',
                                _qualified_class_name(cls))


    def _add_condition(self, key_path, operator,
                       operand=None,
                       value_transformer=None):

        if operand is None:
            bind_type = None
        elif type(operand) in [tuple,list]:
            bind_type = '(%s)' % ','.join(['?'] * len(operand))
        else:
            bind_type = '?'

        clause = (key_path, operator, operand, bind_type, value_transformer)
        self._where.append(clause)

        return self


    def contained_in(self, key_path, values):
        return self._add_condition(key_path, 'IN', values)


    def not_contained_in(self, key_path, values):
        return self._add_condition(key_path, 'NOT IN', values)


    def does_not_exist(self, key_path):
        return self._add_condition(key_path, 'IS NULL')


    def exists(self, key_path):
        return self._add_condition(key_path, 'IS NOT NULL')


    def contains(self, key_path, substr, case_insensitive=False):
        if case_insensitive:
            operand = '%%%s%%' % substr.lower()
            value_transformer = 'lower'
        else:
            operand = '%%%s%%' % substr
            value_transformer = None
        return self._add_condition(key_path, 'LIKE', operand, value_transformer)


    def ends_with(self, key_path, substr, case_insensitive=False):
        if case_insensitive:
            operand = '%%%s' % substr.lower()
            value_transformer = 'lower'
        else:
            operand = '%%%s' % substr
            value_transformer = None
        return self._add_condition(key_path, 'LIKE', operand, value_transformer)


    def starts_with(self, key_path, substr, case_insensitive=False):
        if case_insensitive:
            operand = '%s%%' % substr.lower()
            value_transformer = 'lower'
        else:
            operand = '%s%%' % substr
            value_transformer = None
        return self._add_condition(key_path, 'LIKE', operand, value_transformer)


    def equal_to(self, key_path, value):
        value_transformer = None
        if type(value) in [tuple, list]:
            value = json.dumps(value)
            value_transformer = 'json'
        return self._add_condition(key_path, '=', value, value_transformer)


    def not_equal_to(self, key_path, value):
        value_transformer = None
        if type(value) in [tuple, list]:
            value = json.dumps(value)
            value_transformer = 'json'
        return self._add_condition(key_path, '!=', value, value_transformer)


    def greater_than(self, key_path, n, is_list=False):
        """
        If `is_list`, the operand (`n`) is compared with the *length*
        of the list at `key_path`.
        """

        value_transformer = None
        if is_list:
            value_transformer = 'json_array_length'
        return self._add_condition(key_path, '>', n, value_transformer)


    def greater_than_or_equal_to(self, key_path, n, is_list=False):
        """
        If `is_list`, the operand (`n`) is compared with the *length*
        of the list at `key_path`.
        """

        value_transformer = None
        if is_list:
            value_transformer = 'json_array_length'
        return self._add_condition(key_path, '>=', n, value_transformer)


    def less_than(self, key_path, n, is_list=False):
        """
        If `is_list`, the operand (`n`) is compared with the *length*
        of the list at `key_path`.
        """

        value_transformer = None
        if is_list:
            value_transformer = 'json_array_length'
        return self._add_condition(key_path, '<', n, value_transformer)


    def less_than_or_equal_to(self, key_path, n, is_list=False):
        """
        If `is_list`, the operand (`n`) is compared with the *length*
        of the list at `key_path`.
        """

        value_transformer = None
        if is_list:
            value_transformer = 'json_array_length'
        return self._add_condition(key_path, '<=', n, value_transformer)


    def matches(self, key_path, regex_pattern, case_insensitive=False):
        flags = re.IGNORECASE if case_insensitive else 0
        pattern = re.compile(regex_pattern, flags)

        if self._regexes is None:
            self._regexes = []

        self._regexes.append((key_path, pattern))
        return self


    def matches_query(self, key_path, query):
        """
        An object whose ``id`` matches any of the objects
        in the result set of ``query`` is included in this
        query's result set.

        The value at ``key_path`` is thus a reference which
        is stored as the ``id`` of the referenced object.
        """

        objs = query.find()
        if not objs:
            return self._add_condition(key_path, 'IN', [])

        ids = [obj.id for obj in objs]
        return self.contained_in(key_path, ids)


    def does_not_match_query(self, key_path, query):
        """
        An object whose ``id`` does not match any of the objects
        in the result set of ``query`` is included in this
        query's result set.

        The value at ``key_path`` is thus a reference which
        is stored as the ``id`` of the referenced object.
        """

        objs = query.find()
        if not objs:
            return self._add_condition(key_path, 'IN', [])

        ids = [obj.id for obj in objs]
        return self.not_contained_in(key_path, ids)


    def ascending(self, key_path):
        if self._sort is None:
            self._sort = []

        self._sort.append((key_path, 'ASC'))


    def descending(self, key_path):
        if self._sort is None:
            self._sort = []

        self._sort.append((key_path, 'DESC'))


    def limit(self, n):
        n = int(n)
        if n <= 0:
            raise ValueError()
        self._limit = n
        return self


    def skip(self, n):
        n = int(n)
        if n <= 0:
            raise ValueError()
        self._skip = n
        return self


    def _make_where_sql(self):
        values = []
        clauses = []

        for key_path, operator, operand, bind_type, \
            value_transformer in self._where:

            key_path = _extract(key_path)

            if value_transformer:
                key_path = '%s(%s)' % (value_transformer, key_path)

            if operand is not None:
                clauses.append('%s %s %s' % (
                    key_path, operator, bind_type))

                if type(operand) in [tuple, list]:
                    values.extend(operand)
                elif isinstance(operand, Persistent):
                    values.append(operand.id)
                else:
                    values.append(operand)
            else:
                clauses.append('%s %s' % (key_path, operator))

        return ' AND '.join(clauses), values


    def _make_sort_sql(self):
        if not self._sort:
            return ''

        parts = ['%s %s' % (_extract(key_path), order)
                 for key_path, order in self._sort]

        return 'ORDER BY %s' % ', '.join(parts)


    def _make_offset_sql(self):
        return 'OFFSET %s' % self._skip if self._skip > 0 else ''


    def _make_limit_sql(self):
        if self._skip > 0 or self._limit > 0:
            return 'LIMIT %s' % self._limit if self._limit > 0 else 'LIMIT 1e9'
        return ''


    def _make_sql(self, count_only=False):
        if count_only:
            parts = [ 'SELECT count(*) FROM objects' ]
        else:
            parts = [ 'SELECT json FROM objects' ]

        where_sql, values = self._make_where_sql()
        if len(where_sql) > 0:
            parts.append('WHERE %s' % where_sql)

        parts.append(self._make_sort_sql())
        parts.append(self._make_limit_sql())
        parts.append(self._make_offset_sql())

        return ' '.join(parts), values


    def _results(self, count_only=False):
        sql, values = self._make_sql(count_only)
        return database.connection.execute(sql, values or [])


    def find(self):
        """
        Find all matching objects and return them.
        or return None if there were no matches.
        """

        rows = self._results().fetchall()
        if not rows:
            return None

        objs = [database.unpickle(row[0]) for row in rows]

        if self._regexes:
            return self._filter_by_regexes(objs)

        return objs


    def _filter_by_regexes(self, objs):
        # No regex support in SQLite3 so do it in Python;
        # Only keep objects in result set that match all regexes.

        passed = []

        for obj in objs:
            obj_passes = True

            for key_path, pattern in self._regexes:
                val = keypath.value_at_keypath(obj, key_path)

                if type(val) is not str or not pattern.match(val):
                    obj_passes = False
                    break

            if obj_passes:
                passed.append(obj)

        return passed


    def first(self):
        """
        Find first matching object and return it
        or return None if there were no matches.
        """

        self.limit(1)
        objects = self.find()
        return None if objects is None else objects[0]


    def count(self):
        """
        Returns the number of objects that match
        the query.
        """

        row = self._results(count_only=True).fetchone()
        return int(row[0])


class OrQuery(Query):
    """
    A query whose results are the logical "OR"
    of a number of other queries.
    """

    def __init__(self, *queries):
        Query.__init__(self)
        self.queries = queries


    def _make_where_sql(self):
        all_values = []
        all_where = []

        for q in self.queries:
            where_sql, values = q._make_where_sql()
            if len(where_sql) > 0:
                all_where.append('(%s)' % where_sql)
                all_values.extend(values)

        return (' OR '.join(all_where), all_values)
