"""
Python client for emulating a Datomic database system using MongoDB as storage backend.

.. include:: ../doc/introduction.md
.. include:: ../doc/installation.md

# Usage

The following example script shows the basic usage. You can use [MongoDB Compass](https://www.mongodb.com/products/compass)
to interactively inspect the database content (best in table view).

```py
.. include:: ../example.py
```

# Public API

The public API is inspired by the Clojure 
[Datomic Client API](https://docs.datomic.com/client-api/datomic.client.api.html).
"""
from .core import Client, Connection, Database
from .facts import Facts, Datom

__all__ = ['Client', 'Connection', 'Database', 'Facts', 'Datom']
