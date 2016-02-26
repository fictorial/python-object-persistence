Python Object Persistence
=========================

Here, a persistent object is one that persists or lives beyond the life of
a program by being stored in a database in a serialized form.  The object
can be loaded which deserializes its stored form back into its original class
type with all attributes restored

Concepts
--------

- Your regular Python objects are serialized with
  `jsonpickle <http://jsonpickle.github.io>`_
- Serialized objects are stored in a `SQLite3 <http://sqlite.org>`_ database
- Each object must have a globally (across all class types) unique identifier
  in its ``id`` property.
- *References* between persistent objects are supported. See below for details.

Installation
------------

.. code:: bash

    pip install python-object-persistence

System Requirements
-------------------

SQLite 3.9.0 or later is required for the JSON functionality which was added
about 2 months prior to this writing.

**Python's standard library version is older than the required minimum SQLite3 version.**

However, *Python will use whatever SQLite3 version you have installed*.  Thus, if
you update SQLite3 to the latest stable version you are good to go.

Eventually, Python will bundle SQLite 3.9.0+ and none of this will be required.

OS X
~~~~

Install `Homebrew <http://brew.sh>`_ if you haven't already.

.. code:: bash

    $ brew update
    $ brew install sqlite3 --with-json1

    $ python
    >>> import sqlite3
    >>> sqlite3.sqlite_version
    '3.11.0'

Linux
~~~~~

Install latest SQLite3 from source to home directory:

.. code:: bash

    $ LOCAL=$HOME/local
    $ sqlite_version=3110000  # or whatever
    $ wget http://sqlite.org/2016/sqlite-autoconf-${sqlite_version}.tar.gz -O- | tar xzv
    $ cd sqlite-autoconf-${sqlite_version}
    $ ./configure --enable-json1 --prefix=$LOCAL
    $ make
    $ make install

I use `pyenv <https://github.com/yyuu/pyenv>`_ to manage install Python versions.

.. code:: bash

    $ curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash
    $ echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bash_profile
    $ echo 'eval "$(pyenv init -)"' >> ~/.bash_profile
    $ echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bash_profile
    $ echo 'export PYENV_VERSION=3.5.1' >> ~/.bash_profile
    $ source ~/.bash_profile
    $ LD_RUN_PATH=$HOME/local/lib pyenv install $PYENV_VERSION
    $ pyenv version
    3.5.1

    $ python
    >>> import sqlite3
    >>> sqlite3.sqlite_version
    '3.11.0'

Usage
-----

Connect to the database to use via:

.. code:: python

    import persistent

    persistent.connect(db_path=':memory:', debug=True)

Subclass ``Persistent``.  Create objects of your class type.  Call ``save`` on them.

.. code:: python

    class Foo(persistent.Persistent):
        pass

    f = Foo()
    f.bar = 'hello'
    f.save()

Load an object by ``id``:

.. code:: python

    x = persistent.get(f.id)
    assert x.id == f.id
    assert x.bar == f.bar

Make updates and save the object.

.. code:: python

    x.bar = 'monkey'
    assert x.save()

Inter-Object References
-----------------------

A persistent object may refer to another persistent object by setting an
attribute to the referenced object as in any other Python program. By default,
when the source object is saved, a *copy* of referenced objects is saved with
it.

If you would prefer to save an explicit reference, add the source object
attributes that contain references to the source class's ``references``. On
save, the attributes on the source object are stored as the referenced object's
``id``. On load, the source object's ``references`` are scanned and the referenced
objects loaded, replacing the corresponding attribute on the source object.

.. code:: python

    class Bar(persistent.Persistent):
        references = [ 'a_ref' ]

    b = Bar()
    c = Bar()
    c.baz = 'yes'
    b.a_ref = c
    b.save()
    x = persistent.get(b.id)
    assert x.id == b.id
    assert type(x.a_ref) is Bar
    assert x.a_ref.id == c.id
    assert x.a_ref.baz == c.baz

Timestamps
----------

The system automatically adds ``created_at`` and ``updated_at`` which are
``datetime`` objects in UTC.

.. code:: python

    class Baz(persistent.Persistent): pass
    x = Baz()
    x.save()
    from datetime import datetime
    assert type(x.created_at) is datetime
    assert not hasattr(x, 'updated_at')
    x.quux = 'doo'
    x.save()
    assert type(x.updated_at) is datetime

Caching
-------

A least-recently-used (LRU) cache is used to hold the latest copy of each object by
object ``id``.  On a cache miss, the desired object is loaded from the database
and placed into the cache.  If more than N objects (by default, N=1000) objects
are stored in the cache, the least-recently-used object is evicted from the
cache.

To change the default size of the cache, use the ``cache_size`` parameter when
calling ``persistent.connect``.  To disable caching entirely, set the
``cache_size`` to ``0``.

Indexing
--------

To enforce that only a single object may contain some value for a set of "key
paths", create a "unique index":

.. code:: python

    persistent.add_index(['a', 'b.c'], unique=True)

    x = Bar()
    x.a = 1
    x.b = dict(c=1)
    x.save()  # OK

    y = Bar()
    y.a = 1
    y.b = dict(c=1)
    try: y.save()
    except persistent.UniquenessError as err: assert True
        # Fails as y is non-unique for ['a', 'b.c']

Note that such an index is scoped to the same object class.  If you wish to
make the index span all persistent objects stored, pass ``global_scope=True``
to ``add_index``.

By default, an index has a generated name which is returned by ``add_index``.

A non-unique index can be created to speed up queries.

Querying
--------

To query or find objects, create a ``persist.Query`` object, passing the class of
object. Only objects of the given class will be returned.

.. code:: python

    q = persist.Query(Bar)
    q.equal_to(key_path, value)
    objects = q.find()

Key Paths
~~~~~~~~~

A *key path* is a string with elements separated by a period (.).
Following a key path in an object leads to a particular value.
The value at a key path is what is used as the test value.

Consider key path "a.b.c":

.. code:: python

    o = Persistent()
    o.a = dict(b=dict(c=1))

The value at the key path "a.b.c" is ``1``

See `keypath <https://github.com/fictorial/keypath>`_ for more details.

Filters
~~~~~~~

.. code:: python

    q.equal_to(key_path, value)
    q.not_equal_to(key_path, value)

    q.exists(key_path)
    q.does_not_exist(key_path)

    q.contained_in(key_path, values)
    q.not_contained_in(key_path, values)

    q.starts_with(key_path, substr, case_insensitive=False)
    q.contains(key_path, substr, case_insensitive=False)
    q.ends_with(key_path, substr, case_insensitive=False)

    q.greater_than(key_path, n, is_list=False)
    q.greater_than_or_equal_to(key_path, n, is_list=False)

    q.less_than(key_path, n, is_list=False)
    q.less_than_or_equal_to(key_path, n, is_list=False)

    q.matches(key_path, regex_pattern, case_insensitive=False)

Note: when ``is_list`` is ``True`` the test/comparison is between the *length* of
the list at ``key_path`` and the operand ``n``.

Sorting
~~~~~~~

.. code:: python

    q.ascending(key_path)
    q.descending(key_path)

These can be called multiple times to sort on multiple key paths.

Pagination
~~~~~~~~~~

.. code:: python

    q.limit(n)
    q.skip(n)

Running the Query
~~~~~~~~~~~~~~~~~

.. code:: python

    objs = q.find()
    obj = q.first()
    n = q.count()

``find`` and ``first`` return ``None`` if no object(s) were found.

AND or OR Queries
~~~~~~~~~~~~~~~~~

A ``Query`` is in effect an *AND* query in that all conditions specified
must be met by an object for that object to be included in the result set.

To create an *OR* query, use ``OrQuery``:

.. code:: python

    q = OrQuery(
      Query(A).equal_to('foo', 'bar'),
      Query(B).equal_to('baz', 'buz'),
      Query(C).equal_to('buz', 'quux'))

    objs = q.find()

You may pass an arbitrary number of queries to an ``OrQuery``.

Debugging
---------

Pass ``debug=True`` to ``persistent.connect`` and submitted SQL statements will be
logged using Python's built-in ``logging`` module at the ``debug`` level.

Development
-----------

Run ``make init`` to install Python package dependencies with `pip <https://pip.pypa.io/en/stable>`_.

Testing
-------

Run ``make test`` to run the test suite with `pytest <http://pytest.org/latest/>`_ including coverage reporting.

I aim for 100% code coverage in tests.  See tests.py.

When Python's standard library version of SQLite3 is updated, I will include Tox reports here.
