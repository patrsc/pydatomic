# Introduction

## What is pydatomic?

Pydatomic is a [Python](https://www.python.org/) package that emulates the behavior of a database system inspired by [Datomic](https://www.datomic.com/). This client is **not** able to communicate with a real Datomic server, but uses the free database software [MongoDB](https://www.mongodb.com/) as its storage backend. The database behavior, which is similar to Datomic, is implemented on the client side. The server-side part is a standard MongoDB server, which is just used for storing the database information.

## Why pydatomic?

[Datomic](https://www.datomic.com/) is a unique and innovative database concept that explicitly considers time and is able to work with the database as a value (immutable version of the database at a given point in time). The author of this package is fascinated by the elagance of Datomic's design. However, Datomic is a commercial database system and it is not very easy to get started with it if one is not familiar with the [Clojure](https://clojure.org/) ecosystem.

The motivation behind this project was to provide an easy way to get started with a minimalistic version of such a database system using a commonly used programming language such as Python and a widely used, free, open source database software such as MongoDB. This project is not aiming to provide a replacement for Datomic, but provide a way to use such a system in small private projects that require a time-aware database solution.

## Limitations

Pydatomic is in a very early stage of development. Note that the current version of this project is a **proof of concept** and does **not** provide the full feature set of real Datomic. Only a limited subset of basic functionalities are implemented.
* Not optimized for performance, do not use this in a production environment!
* Complex queries are currently not possible
* Currently no transactor: the client performs transactions directly to the MongoDB backend, validation of data is done *only* on the client side and simultaneous requests from several clients can lead to an illegal database state

## Supported features

Pydatomic is essentially based on the [Datomic Data Model](https://docs.datomic.com/cloud/whatis/data-model.html).

Pydatomic shares the same understanding as Datomic with the following concepts:
* Database
* Datom
* Entities
* Schema
* Time Model
* Identity (with the limitation that *db/id* is not a supported way to refer to an entity)
* Uniqueness (with the limitation that currently *db.unique/identity* and *db.unique/value* have the same semantics)
* Lookup Refs (a list with `[attribute, value]` to identify an entity)

As Python has no *keyword* type, strings are used to store keywords in both Python and MongoDB (without the leading colon, e.g. `"db/ident"`).

As Datomic, Pydatomic uses a [Schema](https://docs.datomic.com/cloud/schema/schema-reference.html) to model domain attributes. In Pydatomic only a limited subset of builtin schema attributes are available, which are:
* `"db/txInstant"`
* `"db/ident"`
* `"db/valueType"`
* `"db/cardinality"` (possible values `"db.cardinality/one"`, `"db.cardinality/many"`)
* `"db/unique"` (possible values missing or `"db.unique/identity"`, `"db.unique/value"`)
* `"db/doc"`

### Value Types

Only a limited subset of Datomic's value types are supported:

| Value type          | Python type | MongoDB type |
|---------------------|-------------|--------------|
| `"db.type/boolean"` | `bool`      | `Boolean`    |
| `"db.type/double"`  | `float`     | `Double`     |
| `"db.type/instant"` | `int`       | `Date`       |
| `"db.type/keyword"` | `str`       | `String`     |
| `"db.type/long"`    | `int`       | `Int64`      |
| `"db.type/ref"`     | `int`       | `Int64`      |
| `"db.type/string"`  | `str`       | `String`     |
| `"db.type/uuid"`    | `str`       | `String`     |
| `"db.type/uri"`     | `str`       | `String`     |

Comments:
* **instant**: represented in Python by an `int` (milliseconds since epoch UTC)
* **keyword**: the string must match a certain regular expression, which is more restrictive than keywords in Clojure: a keyword must either be an *identifier* (regular expression `[a-zA-Z][a-zA-Z0-9-_]*`), or a *namespace* followed by `/` (slash) followed by an *identifier*; a *namespace* is a sequence of at least one *identifier* separated by `.` (dot)
* **uuid**: lowercase string representation of a UUID, e.g. `"43e757fa-1db8-4a92-abce-3eddd2c1ef93"`, will be validated
* **uri**: string validated by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986/) specification

The following value types are currently **not** supported:
* `"db.type/bigdec"`
* `"db.type/bigint"`
* `"db.type/float"` (use `"db.type/double"` instead)
* `"db.type/symbol"`
* `"db.type/tuple"`

## Disclaimer

Datomic® is a registered trademark of Cognitect, Inc. **The author(s) of this software is/are not affiliated with Cognitect or Datomic.** The trademarks DATOMIC® and COGNITECT™ and related rights are the sole property of Cognitect, Inc. http://cognitect.com/
