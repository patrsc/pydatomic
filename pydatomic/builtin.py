import re
from enum import Enum
from pydantic import BaseModel, root_validator
from typing import Optional, Any
from .facts import Datom
from bson.int64 import Int64
from fastapi.encoders import jsonable_encoder
from .util import datetime, int_to_datetime, datetime_to_int
from .exceptions import EntityNotFoundError
import uuid
from rfc3986 import is_valid_uri
from datetime import timezone


class ValueTypeDef(BaseModel):
    """container for attributes of a value type"""
    name: str
    python_type: type
    mongo_type: str


class ValueType(Enum):
    """set of value types
    
    currently NOT supported types are:
    * db.type/bigdec
    * db.type/bigint
    * db.type/float
    * db.type/symbol
    * db.type/tuple
    """
    boolean  = ValueTypeDef(name='db.type/boolean', python_type=bool,  mongo_type='Boolean')
    double   = ValueTypeDef(name='db.type/double',  python_type=float, mongo_type='Double')
    instant  = ValueTypeDef(name='db.type/instant', python_type=int,   mongo_type='Date')
    keyword  = ValueTypeDef(name='db.type/keyword', python_type=str,   mongo_type='String')
    long     = ValueTypeDef(name='db.type/long',    python_type=int,   mongo_type='Int64')
    ref      = ValueTypeDef(name='db.type/ref',     python_type=int,   mongo_type='Int64')
    string   = ValueTypeDef(name='db.type/string',  python_type=str,   mongo_type='String')
    uuid     = ValueTypeDef(name='db.type/uuid',    python_type=str,   mongo_type='String')
    uri      = ValueTypeDef(name='db.type/uri',     python_type=str,   mongo_type='String')

    @classmethod
    def values(cls):
        return [v.value.name for v in cls]

    @staticmethod
    def to_dict():
        return {v.value.name: v for v in ValueType}

    def validate_type(self, value: Any, attribute_name: str, db):
        """validate that given value has correct value type"""
        pt = self.value.python_type
        if not isinstance(value, pt):
            raise ValueError(f'the attribute {attribute_name!r} has value type {self.value.name!r}, expected Pyton type is {pt.__name__}, got {type(value).__name__} instead')
        if self == ValueType.keyword:
            validate_keyword(value)
        elif self == ValueType.ref:
            d = db.get(value)
            if d == {}:
                raise ValueError(f'entity {value} does not exist: a reference must point to a valid entity that has at least one attribute set')
        elif self == ValueType.uuid:
            validate_uuid(value)
        elif self == ValueType.uri:
            validate_uri(value)

    def mongo_encode_value(self, value: Any):
        if self.value.mongo_type == 'Date':
            v = int_to_datetime(value)
        elif self.value.mongo_type == 'Int64':
            v = Int64(value)
        else:
            v = value
        return v

    def mongo_encode(self, datom: Datom) -> dict[str,Any]:
        d = jsonable_encoder(datom)
        for k in ['_id', 'e', 'tx']:
            d[k] = Int64(d[k])
        d['v'] = self.mongo_encode_value(d['v'])
        return d

    @staticmethod
    def mongo_decode(mongo_datom: dict[str,Any]) -> Datom:
        if isinstance(mongo_datom['v'], datetime):
            t = mongo_datom['v'].replace(tzinfo=timezone.utc)
            mongo_datom['v'] = datetime_to_int(t)
        return Datom(**mongo_datom)


class Cardinality(Enum):
    """set of cardinality values"""
    one = 'db.cardinality/one'    # single value
    many = 'db.cardinality/many'  # a set of values (unordered, no value repetitions)

    @classmethod
    def values(cls):
        return [v.value for v in cls]


class Unique(Enum):
    """set of uniqueness values"""
    identity = 'db.unique/identity'
    val = 'db.unique/value'

    @classmethod
    def values(cls):
        return [v.value for v in cls]


class Attr(BaseModel):
    """represents a Datomic attribute"""
    ident: str
    value_type: ValueType
    cardinality: Cardinality
    unique: Optional[Unique] = None
    doc: str = ''
    restricted_values: Optional[list[str]] = None

    @root_validator(pre=True)
    def unique_cardinality(cls, values):
        if 'unique' in values and values['unique'] is not None:
            c = Cardinality(values['cardinality'])
            u = Unique(values['unique'])
            if (u in [Unique.identity, Unique.val]) and c != Cardinality.one:
                name = values['ident']
                raise ValueError(f'attribute {name!r} is set to be unique, so it must have cardinality one')
        return values

    @staticmethod
    def builtin():
        """returns a list of builtin attributes"""
        return builtin_attr
    
    @staticmethod
    def builtin_dict():
        """returns a dict of builtin attributes"""
        return builtin_attr_dict

    @staticmethod
    def from_dict(dct):
        u = dct['db/unique'] if 'db/unique' in dct else None
        d = dct['db/doc'] if 'db/doc' in dct else ''
        required = ['db/valueType', 'db/cardinality']
        name = dct['db/ident']
        for req in required:
            if req not in dct:
                raise ValueError(f'required attributte {req!r} of attribute {name!r} is not defined')
        vt = ValueType.to_dict()[dct['db/valueType']]
        return Attr(ident=name, value_type=vt, cardinality=dct['db/cardinality'], unique=u, doc=d)

    def validate_value(self, value: Any, db):
        self.value_type.validate_type(value, self.ident, db)
        self.validate_restricted_values(value)

    def validate_restricted_values(self, value: Any):
        if self.restricted_values is not None:
            if not (isinstance(value, str) and value in self.restricted_values):
                raise ValueError(f'the attribute {self.ident!r} must be one of the values {set(self.restricted_values)}, got {value!r} instead')

    def validate_cardinality(self, e: int, value: Any, db, op: bool):
        """validate cardinality (considering all retracted facts)"""
        data = db.get(e)
        if op:
            # fact is added
            if self.cardinality == Cardinality.one:
                # if cardinality is one, the entity e cannot have the attribute set to any value
                if self.ident in data:
                    raise ValueError(f'cannot add attribute {self.ident!r} of entity {e}: a value is already set (cardinality is one)')
            else:
                # if cardinality is many, the entity e cannot have the attribute set to the given value
                if self.ident in data:
                    values = set(data[self.ident])
                    if value in values:
                        raise ValueError(f'cannot add attribute {self.ident!r} of entity {e}: the value {value!r} is already present (cardinality is many)')
        else:
            # fact is retracted
            if self.cardinality == Cardinality.one:
                # if cardinality is one, the entity e must have the attribute set to the given value
                if self.ident not in data or data[self.ident] != value:
                    raise ValueError(f'cannot remove attribute {self.ident!r} of entity {e}: the value {value!r} is not set (cardinality is one)')
            else:
                # if cardinality is many, the entity e must have the given value in its set of values
                if self.ident not in data or value not in set(data[self.ident]):
                    raise ValueError(f'cannot remove attribute {self.ident!r} of entity {e}: the value {value!r} is not set (cardinality is many)')

    def validate_uniqueness(self, value: Any, db, op: bool):
        """validate uniqueness (considering all retracted facts)"""
        if op and self.unique in [Unique.identity, Unique.val]:
            # no other entity can have this attribute set to the given value
            try:
                e = db._lookup(self.ident, value)
                found = True
            except EntityNotFoundError:
                e = None
                found = False
            if found:
                raise ValueError(f'cannot set unique attribute {self.ident!r} to {value!r}, because this value is already assigned to entity {e}')


def validate_keyword(value: str):
    """
    Clojure, edn and Datomic define a keyword in quite loose terms.
    The definition used here is more strict:

    * identifier = [a-zA-Z][a-zA-Z0-9-_]*
    * namespace = {identifier}(.{identifier})*
    * keyword = ({namespace}/)?{identifier}
    """
    keyword_regex = r'^((?P<namespace>([a-zA-Z][a-zA-Z0-9-_]*)(\.[a-zA-Z][a-zA-Z0-9-_]*)*)/)?(?P<identifier>[a-zA-Z][a-zA-Z0-9-_]*)$'
    m = re.fullmatch(keyword_regex, value)
    if m is None:
        raise ValueError(f'the value {value!r} is not a valid keyword value')
    d = m.groupdict()
    ns = d['namespace']
    namespace = [] if ns is None else ns.split('.')
    identifier = d['identifier']
    return namespace, identifier


def validate_uuid(value: str):
    if not is_valid_lowercase_uuid(value):
        raise ValueError(f'the value {value!r} is not a valid lowercase UUID')


def is_valid_lowercase_uuid(value: str):
    if value != value.lower():
        return False
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False


def validate_uri(value: str):
    if not is_valid_uri(value):
        raise ValueError(f'the value {value!r} is not a valid URI')


# Module-level variables for performance
builtin_attr = [
    Attr(ident='db/txInstant', value_type=ValueType.instant, cardinality=Cardinality.one),
    Attr(ident='db/ident', value_type=ValueType.keyword, cardinality=Cardinality.one, unique=Unique.identity),
    Attr(ident='db/valueType', value_type=ValueType.keyword, cardinality=Cardinality.one, restricted_values=ValueType.values()),
    Attr(ident='db/cardinality',value_type=ValueType.keyword, cardinality=Cardinality.one, restricted_values=Cardinality.values()),
    Attr(ident='db/unique', value_type=ValueType.keyword, cardinality=Cardinality.one, restricted_values=Unique.values()),
    Attr(ident='db/doc', value_type=ValueType.string, cardinality=Cardinality.one),
]
builtin_attr_dict = {a.ident: a for a in builtin_attr}
