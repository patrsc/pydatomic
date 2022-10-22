"""
Python client for emulating a Datomic database system using MongoDB as storage backend.

.. include:: ../pydatomic.md

# Usage

The following example script shows the basic usage. You can use [MongoDB Compass](https://www.mongodb.com/products/compass)
to interactively inspect the database content (best in table view).

```py
.. include:: ../pydatomic_test.py
```

# Public API
"""
from .core import Client, Connection, Database
from .facts import Facts, Datom

__version__ = "0.1.0"
__all__ = ['Client', 'Connection', 'Database', 'Facts', 'Datom']
