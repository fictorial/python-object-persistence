import os
import sqlite3
from datetime import datetime, timedelta

import pytest

import persistent


class A(persistent.Persistent):
    pass


class B(persistent.Persistent):
    references = [ 'ref0' ]


class C(persistent.Persistent):
    references = [ 'ref0', 'ref1' ]


def test_connect():
    persistent.connect()


def test_connect_debug():
    persistent.connect(debug=True)


def test_connect_no_cache():
    persistent.connect(cache_size=0)


def test_connect_file():
    try:
        persistent.connect(db_path='.test.sqlite3')
    finally:
        try:
            os.remove('.test.sqlite3')
        except:
            pass


def test_subclass_create():
    persistent.connect(debug=True)
    a = A()


def test_dirty():
    persistent.connect(debug=True)
    a = A()
    assert a.is_dirty


def test_mark_clean():
    persistent.connect(debug=True)
    a = A()
    assert a.is_dirty
    a.mark_clean()
    assert not a.is_dirty
    a.mark_clean()
    assert not a.is_dirty


def test_save_empty_object():
    persistent.connect(debug=True)
    a = A()
    a.save()

def test_save_empty_object_not_dirty():
    persistent.connect(debug=True)
    a = A()
    a.save()
    assert not a.is_dirty
    assert a.save() == a


def test_save_and_load_empty():
    persistent.connect(debug=True)
    a = A()
    a.save()
    b = persistent.get(a.id)
    assert b.id == a.id


def test_save_and_load_with_attribs():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    b = persistent.get(a.id)
    assert b.id == a.id
    assert b.foo == a.foo


def test_save_and_load_with_attribs_multi():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    a.bar = 2
    a.save()  # not new so update
    b = persistent.get(a.id)
    assert b.id == a.id
    assert b.foo == a.foo


def test_with_references():
    persistent.connect(debug=True)
    b = B()
    c = A()
    c.foo = 1
    b.foo = 2
    b.ref0 = c
    bp = b._with_references()
    assert type(bp.ref0) is str
    assert bp.ref0 == c.id
    assert b.save()   # c will be saved too
    assert not b.is_dirty
    assert not c.is_dirty


def test_refs_save_reload():
    persistent.connect(debug=True)
    b = B()
    c = A()
    c.foo = 1
    b.foo = 2
    b.ref0 = c
    b.save()
    bp = persistent.get(b.id)
    assert isinstance(bp.ref0, A)
    assert bp.ref0.id == c.id


def test_refs_save_reload_multi():
    persistent.connect(debug=True)
    b = B()
    c = A()
    c.foo = 1
    b.foo = 2
    b.ref0 = c
    b.save()
    b.bar = 1
    b.save()  # not new so trigger update
    bp = persistent.get(b.id)
    assert isinstance(bp.ref0, A)
    assert bp.ref0.id == c.id


def test_multiple_refs_save_reload():
    persistent.connect(debug=True)
    c = C()
    a0 = A()
    a0.foo = 1
    a1 = A()
    a1.foo = 2
    c.foo = 3
    c.ref0 = a0
    c.ref1 = a1
    c.save()
    cp = persistent.get(c.id)
    assert isinstance(cp.ref0, A)
    assert isinstance(cp.ref1, A)
    assert cp.ref0.id == a0.id
    assert cp.ref1.id == a1.id
    assert cp.ref0.foo == a0.foo
    assert cp.ref1.foo == a1.foo


def test_get_unknown():
    persistent.connect(debug=True)
    with pytest.raises(persistent.NotFoundError):
        persistent.get('whatever')


def test_unique_index():
    persistent.connect(debug=True)
    persistent.add_index(['a', 'b.c'], unique=True)
    x = A()
    x.a = 1
    x.b = dict(c=1)
    x.save()  # OK
    y = A()
    y.a = 1
    y.b = dict(c=1)
    with pytest.raises(persistent.UniquenessError):
        y.save()


def test_query_can_create():
    persistent.Query(A)


def test_query_can_create_without_class():
    persistent.Query()


def test_query_all_objects_one():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    objects = persistent.Query().find()
    assert objects[0].id == a.id


def test_query_all_objects_many():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objects = persistent.Query().find()
    assert objects[0].id == a0.id
    assert objects[1].id == a1.id


def test_query_all_A_objects_one():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    objects = persistent.Query(A).find()  # scoped
    assert objects[0].id == a.id


def test_query_all_A_objects_many():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objects = persistent.Query(A).find()
    assert objects[0].id == a0.id
    assert objects[1].id == a1.id


def test_query_first_A_objects_many():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    obj = persistent.Query(A).first()
    assert obj.id == a0.id


def test_exists():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    objs = persistent.Query(A).exists('foo').find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_does_not_exist():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.bar = 1
    a1.save()
    objs = persistent.Query(A).does_not_exist('foo').find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_equal_to():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 1
    a1.save()
    objs = persistent.Query(A).equal_to('foo', 1).find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_equal_to_list():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1,2,3]
    a0.save()
    a1 = A()
    a1.foo = 1
    a1.save()
    objs = persistent.Query(A).equal_to('foo', [1,2,3]).find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_not_equal_to():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).not_equal_to('foo', 1).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_not_equal_to_list():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1,2,3]
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).not_equal_to('foo', [1,2,3]).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_greater_than():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).greater_than('foo', 1).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_greater_than_with_list():
    """With a list, the operand tests against the *length* of the list"""
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1,2,3]
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).greater_than('foo', 2, is_list=True).find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_greater_than_or_equal_to():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    a2 = A()
    a2.foo = 0
    a2.save()
    objs = persistent.Query(A).greater_than_or_equal_to('foo', 1).find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_greater_than_or_equal_to_with_list():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1]
    a0.save()
    a1 = A()
    a1.foo = [1,2,3]
    a1.save()
    a2 = A()
    a2.foo = [1,2]
    a2.save()
    q = persistent.Query(A)
    q.greater_than_or_equal_to('foo', 2, is_list=True)
    objs = q.find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a1.id in ids
    assert a2.id in ids


def test_less_than():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).less_than('foo', 2).find()
    assert len(objs) == 1
    assert objs[0].id == a0.id

def test_less_than_with_list():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1,2,3]
    a0.save()
    a1 = A()
    a1.foo = [1,2]
    a1.save()
    objs = persistent.Query(A).less_than('foo', 3, is_list=True).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_less_than_or_equal_to():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).less_than_or_equal_to('foo', 2).find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_less_than_or_equal_to_with_list():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = [1,2,3]
    a0.save()
    a1 = A()
    a1.foo = [1,2]
    a1.save()
    a2 = A()
    a2.foo = [1,2,3,4]
    a2.save()
    objs = persistent.Query(A).less_than_or_equal_to('foo', 3, is_list=True).find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_contained_in():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).contained_in('foo', (1,3,5)).find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_not_contained_in():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    objs = persistent.Query(A).not_contained_in('foo', (1,3,5)).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_contains_str():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).contains('foo', 'c').find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_starts_with_str():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).starts_with('foo', 'ab').find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_ends_with_str():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).ends_with('foo', 'de').find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_contains_str_case_insensitive():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).contains('foo', 'C', case_insensitive=True).find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a1.id in ids


def test_starts_with_str_case_insensitive():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).starts_with('foo', 'aB', case_insensitive=True).find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_ends_with_str_case_insensitive():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).ends_with('foo', 'DE', case_insensitive=True).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_matches_regex():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).matches('foo', r'^a').find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_matches_regex_case_insensitive():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 'abc'
    a0.save()
    a1 = A()
    a1.foo = 'cde'
    a1.save()
    objs = persistent.Query(A).matches('foo', r'^CDE$', case_insensitive=True).find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_sort_ascending_one():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    q.ascending('foo')
    objs = q.find()
    assert len(objs) == 2
    assert objs[0].id == a0.id
    assert objs[1].id == a1.id


def test_sort_descending_one():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    q.descending('foo')
    objs = q.find()
    assert len(objs) == 2
    assert objs[0].id == a1.id
    assert objs[1].id == a0.id


def test_sort_ascending_multi():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.bar = 0
    a0.save()
    a1 = A()
    a1.foo = 1
    a1.bar = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    q.ascending('foo')
    q.ascending('bar')
    objs = q.find()
    assert len(objs) == 2
    assert objs[0].id == a0.id
    assert objs[1].id == a1.id


def test_sort_descending_multi():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.bar = 0
    a0.save()
    a1 = A()
    a1.foo = 1
    a1.bar = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    q.descending('foo')
    q.descending('bar')
    objs = q.find()
    assert len(objs) == 2
    assert objs[0].id == a1.id
    assert objs[1].id == a0.id

def test_query_skip():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    q.descending('foo')
    q.skip(1)
    objs = q.find()
    assert len(objs) == 1
    assert objs[0].id == a0.id


def test_query_count():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('foo')
    assert q.count() == 2


def test_query_count_nothing():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('bar')
    assert q.count() == 0


def test_query_find_nothing():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    q = persistent.Query(A)
    q.exists('bar')
    assert q.find() == None


def test_query_invalid_limit():
    persistent.connect(debug=True)
    q = persistent.Query(A)
    with pytest.raises(ValueError):
        q.limit(-1)


def test_query_invalid_skip():
    persistent.connect(debug=True)
    q = persistent.Query(A)
    with pytest.raises(ValueError):
        q.skip(-1)


def test_or_query():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    a2 = A()
    a2.bar = 2
    a2.save()
    q1 = persistent.Query(A).exists('bar')
    q2 = persistent.Query(A).equal_to('foo', 1)
    q = persistent.OrQuery(q1, q2)
    objs = q.find()
    assert len(objs) == 2
    ids = [obj.id for obj in objs]
    assert a0.id in ids
    assert a2.id in ids


def test_query_multiple_AND_conditions():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.bar = 2
    a0.save()
    a1 = A()
    a1.foo = 1
    a1.bar = 3
    a1.save()
    q = persistent.Query(A)
    q.equal_to('foo', 1)
    q.greater_than('bar', 2)
    objs = q.find()
    assert len(objs) == 1
    assert objs[0].id == a1.id


def test_should_fail_when_dimwit_alters_table():
    import persistent.database
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    persistent.database.connection.execute("""
        ALTER TABLE objects RENAME TO whatever
    """)
    with pytest.raises(sqlite3.OperationalError):
        a0.save()


def test_delete():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.save()
    a1 = persistent.get(a0.id)
    assert a1.id == a0.id
    a0.delete()
    with pytest.raises(persistent.NotFoundError):
        persistent.get(a0.id)


def test_transaction():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a1 = A()
    a1.foo = 1
    with persistent.transaction():
        a0.save(use_transaction=False)
        a1.save(use_transaction=False)
    assert persistent.get(a0.id).id == a0.id
    assert persistent.get(a1.id).id == a1.id


def test_transaction_delete():
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a1 = A()
    a1.foo = 1
    with persistent.transaction():
        a0.delete(use_transaction=False)
        a1.save(use_transaction=False)
    assert persistent.get(a1.id).id == a1.id
    with pytest.raises(persistent.NotFoundError):
        persistent.get(a0.id)


def test_query_ref():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    b = B()
    b.ref0 = a
    b.save()
    q = persistent.Query(B)
    q.equal_to('ref0', a)
    assert q.count() == 1


def test_query_matches_query():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    # b0 references a
    b0 = B()
    b0.foo = 2
    b0.ref0 = a
    b0.save()
    # No reference for b1
    b1 = B()
    b1.foo = 2
    b1.save()
    # Find all As where foo==1
    qa = persistent.Query(A)
    qa.equal_to('foo', 1)
    # Find all Bs that have a reference to any
    # of the objects matched by query qa
    qb = persistent.Query(B)
    qb.matches_query('ref0', qa)
    objs = qb.find()
    assert len(objs) == 1
    assert objs[0].id == b0.id


def test_query_does_not_match_query():
    persistent.connect(debug=True)
    a = A()
    a.foo = 1
    a.save()
    a1 = A()
    a1.foo = 2
    a1.save()
    # b0 references a
    b0 = B()
    b0.foo = 2
    b0.ref0 = a
    b0.save()
    # No reference for b1
    b1 = B()
    b1.foo = 2
    b1.save()
    # Find all As where foo==1
    qa = persistent.Query(A)
    qa.equal_to('foo', 1)
    # Find all Bs that do not have a reference to any
    # of the objects matched by query qa
    no_ref = persistent.Query(B)
    no_ref.does_not_exist('ref0')
    in_qa = persistent.Query(B)
    in_qa.does_not_match_query('ref0', qa)
    q = persistent.OrQuery(no_ref, in_qa)
    objs = q.find()
    assert len(objs) == 1
    assert objs[0].id == b1.id


def test_query_dates():
    # isodatetimehandler stores datetime objects
    # as ISO-8601 formatted text. Thus, we can use
    # regular SQL comparisons on key paths storing
    # datetimes in our queries.
    persistent.connect(debug=True)
    a0 = A()
    a0.foo = 1
    a0.a_date = datetime.utcnow() - timedelta(hours=1)
    a0.save()
    a1 = A()
    a1.foo = 2
    a1.a_date = datetime.utcnow() - timedelta(hours=1, minutes=1)
    a1.save()
    assert persistent.Query(A).less_than('a_date', datetime.utcnow()).count() == 2

