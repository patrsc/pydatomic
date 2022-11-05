from pymongo import MongoClient, InsertOne, ASCENDING
from typing import Collection, Optional, Union, Any
from .facts import Facts, Datom
from .util import now, datetime, datetime_to_int
from .builtin import Attr, Cardinality, ValueType
from .exceptions import EntityNotFoundError


default_uri = 'mongodb://127.0.0.1:27017'


class Client:
    """represents a link to a remote server that stores databases"""

    def __init__(self, uri: str = default_uri) -> None:
        """create a new Client object
        
        :param uri: the URI to the MongoDB server
        :return: the Client object
        """
        self._uri = uri
        self._client = MongoClient(uri)

    def create_database(self, db_name: str):
        """create a new database on the server
        :param db_name: name that identifies the database
        """
        if db_name in self.list_databases():
            raise ValueError(f'database {db_name} already exists')
        self._client[db_name].create_collection('datoms')
        self._create_indices(db_name)

    def delete_database(self, db_name: str):
        """delete an existing database on the server
        :param db_name: name that identifies the database
        """
        if db_name not in self.list_databases():
            raise ValueError(f'database {db_name} does not exist')
        self._client.drop_database(db_name)

    def list_databases(self) -> list[str]:
        """get a list of databases on the server
        :return: the list of database names
        """
        mongo_dbs = self._client.list_databases()
        dbs = []
        for db in mongo_dbs:
            if 'datoms' in self._client[db['name']].list_collection_names():
                dbs.append(db['name'])
        return dbs

    def connect(self, db_name: str) -> 'Connection':
        """connect to a server database
        :return: the Connection object
        """
        if db_name not in self.list_databases():
            raise ValueError(f'database {db_name} does not exist')
        self._create_indices(db_name)
        return Connection(self, db_name)

    def close(self):
        """close client and connections to server"""
        self._client.close()

    def __del__(self):
        self.close()
    
    def _create_indices(self, db_name):
        coll = self._client[db_name]['datoms']
        indexes = coll.index_information()
        for name in ['e', 'a', 'tx']:
            if name not in indexes:
                coll.create_index([(name, ASCENDING)], name=name, background=True)
        if 'av' not in indexes:
            coll.create_index([('a', ASCENDING), ('v', ASCENDING)], name='av', background=True)


class Connection:
    """represents a connection to a remote database"""

    def __init__(self, client: Client, db_name: str):
        """create Connection object (recommended to use Client.connect instead)"""
        self._db_name = db_name
        self._client = client
        self._datoms = self._client._client[db_name]['datoms']

    def db(self) -> 'Database':
        """returns the current database value (the latest database on the server)"""
        d = self._datoms.find_one({'$query': {}, '$orderby': {'tx': -1}})
        tx_max = -1 if d is None else d['tx']
        r = RemoteDatabase(self, tx_max)
        return Database(remote=r, tx_min=-1, with_datoms=None, full_history=False)
    
    def transact(self, facts: Facts) -> tuple['Database', 'Database', list[Datom], dict[str,int]]:
        """transact a new set of facts to the database on the server

        :param facts: the set of facts to be transacted
        :return:
          - db_before - the database value before the transaction was executed
          - db_after - the database value after the transaction was executed
          - tx_data - the transaction data as a list of pydatomic.Datom objects
          - temp_ids - a dictionary of resolved named temporary entity IDs
        """
        db_before = self.db()
        tx_data, temp_ids = db_before._transaction_data(facts)
        operations = []
        lst_mongo = db_before._validate_transaction(tx_data)
        for d in lst_mongo:
            operations.append(InsertOne(d))
        self._datoms.bulk_write(operations)
        db_after = self.db()
        return db_before, db_after, tx_data, temp_ids

class Database:
    """represents a database value"""

    def __init__(self, remote: Optional['RemoteDatabase'] = None, tx_min: int = -1, with_datoms: Optional['LocalDatoms'] = None, full_history=False) -> None:
        """create a new Database object from scratch (use this only without arguments to create a new standalone database
        that is not connected to a server, use Connection.db to get the Database value of a server)
        """
        if with_datoms is None:
            with_datoms = LocalDatoms([])
        self._remote = remote  # if None, this is a virtual db assuming an empty remote database
        self._tx_min = tx_min  # exclusive: tx > tx_min
        self._with_datoms: LocalDatoms = with_datoms
        self._full_history = full_history
        self._attr_def_cache = {}  # cache of attributes for performance
        self._attr_index = {}  # cached datoms grouped by attribute: {a->Index([datom where datom.a==a])}
        self._attr_val_index = {}  # cached datoms grouped by attribute/value: {a->v->Index([datom where datom.a==a and datom.v==v])}
        self._entity_index = {}  # cached datoms grouped by entity: {e->Index([datom where datom.e==e])}
        self._attr_val_index_complete = set()  # set of attributes for which the attr_val_index is complete

    @property
    def _remote_tx_max(self):
        return -1 if self._remote is None else self._remote._tx_max

    @property
    def _tx_max(self):
        if len(self._with_datoms) > 0:
            return self._with_datoms.tx_max()
        else:
            return self._remote_tx_max

    def _get_max_id(self):
        return self._get_max_key('_id')

    def _get_max_entity_id(self):
        return self._get_max_key('e')

    def _get_max_key(self, key):
        if self._remote is None:
            v = -1
        else:
            r = self._remote
            tx_max = r._tx_max
            d = r._conn._datoms.find_one({'$query': {'tx': {'$lte': tx_max}}, '$orderby': {key: -1}})
            v = -1 if d is None else d[key]
        vf = self._with_datoms.max_key(key)
        if vf > v:
            v = vf
        return v

    def entities(self):
        """get an Iterable over all entity IDs (int values)"""
        e_max = self._get_max_entity_id()
        return range(0, e_max+1)

    def transaction_at(self, time: Union[int, datetime, str]) -> int:
        """get highest transaction ID (tx_id) that existed at the given timestamp
        :param time: a timestamp represented as int (milliseconds since epoch at UTC), datetime or string in ISO format
        :return: the entity ID of the transaction suitable for passing to functions such as Database.as_of, Database.since
        """
        if isinstance(time, str):
            time = datetime.fromisoformat(time)
        t = datetime_to_int(time) if isinstance(time, datetime) else time
        facts = self._find_attribute_value('db/txInstant', None)
        tx_id = -1
        for f in facts:
            if f.v <= t and f.tx > tx_id:
                tx_id = f.tx
        return tx_id

    def _get_attr_index(self, attribute: str, value):
        facts = []
        if self._remote is not None:
            r = self._remote
            tx_max = r._tx_max
            if value is None:
                docs = r._conn._datoms.find({'a': attribute, 'tx': {'$lte': tx_max}})
            else:
                docs = r._conn._datoms.find({'a': attribute, 'v': value, 'tx': {'$lte': tx_max}})
            for d in docs:
                facts.append(ValueType.mongo_decode(d))
        if value is None:
            for f in self._with_datoms.facts_by_attribute(attribute):
                facts.append(f)
        else:
            for f in self._with_datoms.facts_by_attribute_value(attribute, value):
                facts.append(f)
        return Index(facts)

    def _get_attr_val_index_from_attr_index(self, attr_index: 'Index'):
        values = {}
        for f in attr_index.facts:
            if f.v not in values:
                values[f.v] = Index([])
            values[f.v].facts.append(f)
        return values

    def _find_attribute_value(self, attribute: str, value: Any) -> list[Datom]:
        """return all facts with given (attribute, value) pair, if value is None it finds the attribute with any value"""
        if value is None:
            if attribute not in self._attr_index:
                # add attribute to index for fast future attribute lookup
                self._attr_index[attribute] = self._get_attr_index(attribute, None)
                # create attribute/value index from attribute index
                self._attr_val_index[attribute] = self._get_attr_val_index_from_attr_index(self._attr_index[attribute])
                self._attr_val_index_complete.add(attribute)
        else:
            if attribute not in self._attr_val_index:
                self._attr_val_index[attribute] = {}
            if attribute not in self._attr_val_index_complete and value not in self._attr_val_index[attribute]:
                self._attr_val_index[attribute][value] = self._get_attr_index(attribute, value)
        
        # use index to filter attribute datoms
        if value is None:
            facts = self._attr_index[attribute].facts.copy()
        else:
            if value in self._attr_val_index[attribute]:
                facts = self._attr_val_index[attribute][value].facts.copy()
            else:
                facts = []
        return facts

    def _lookup(self, attribute, value) -> int:
        """get entity id of an entity with a unique attribute"""
        attr_def = self._get_attr_def(attribute)
        if not attr_def.is_unique():
            raise ValueError(f'lookup failed because attribute {attribute!r} is not unique')

        e = self._lookup_direct(attribute, value)
        if e is None:
            raise EntityNotFoundError(f'no entity found with attribute {attribute!r} set to {value!r}')
        return e
    
    def _lookup_direct(self, attribute, value) -> Optional[int]:
        candidates = self._find_candidates(attribute, value, return_all_attributes=False)
        for e, d in candidates.items():
            if attribute in d and d[attribute] == value:
                return e
        return None

    def _transaction_data(self, facts: Facts) -> tuple[list[Datom], dict[str,int]]:
        max_entity = self._get_max_entity_id()
        max_id = self._get_max_id()
        current_max_entity = max_entity
        tx = max_entity + 1
        max_entity += 1
        time = now()
        temp_ids = {'datomic.tx': tx}
        tx_data = []
        f = facts._facts.copy()
        f.insert(0, ('datomic.tx', 'db/txInstant', time, True))
        for i, t in enumerate(f):
            entity, attribute, value, op = t
            entity, temp_ids, max_entity = self._resolve_entity(entity, temp_ids, max_entity, current_max_entity)
            did = max_id + i + 1
            tx_data.append(Datom(_id=did, e=entity, a=attribute, v=value, tx=tx, op=op))
        return tx_data, temp_ids

    def _resolve_entity(self, entity, names, max_entity, current_max_entity):
        if isinstance(entity, int):
            if entity > current_max_entity:
                raise ValueError(f'entity {entity} is not assigned yet')
            e = entity
        elif isinstance(entity, list) and len(entity) == 2:
            e = self._lookup(entity[0], entity[1])
        elif isinstance(entity, str):
            if entity not in names:
                max_entity += 1
                names[entity] = max_entity
            e = names[entity]
        elif entity is None:
            max_entity += 1
            e = max_entity
        else:
            raise ValueError('an entity must be an int, a string (temporary named new entity), None (unnamed new entity), or a lookup ref (list)')
        return e, names, max_entity

    def as_of(self, tx_id) -> 'Database':
        """returns a database value from the beginning until a point in time represented by a transaction ID
        :param tx_id: the transaction ID (inclusive)
        :return: the database value with transactions tx <= tx_id
        """
        if tx_id > self._tx_max:
            raise ValueError('cannot travel into the future')
        if tx_id > self._remote_tx_max:
            r = self._remote
            f = self._with_datoms.as_of(tx_id)
        else:
            r = None if self._remote is None else RemoteDatabase(self._remote._conn, tx_id)
            f = None
        return Database(remote=r, tx_min=-1, with_datoms=f, full_history=False)

    def since(self, tx_id) -> 'Database':
        """returns a database value after a point in time represented by a transaction ID
        :param tx_id: the transaction ID (exclusive)
        :return: the database value with transactions tx > tx_id
        """
        return Database(remote=self._remote, tx_min=tx_id, with_datoms=self._with_datoms, full_history=False)

    def history(self) -> 'Database':
        """returns a history database value of the given database value
        (currently history has no effect, since complex queries are not implemented yet; 
        to get all historic facts the functions Database.facts and Database.all_facts can be used on both history databases and normal databases)
        :return: the database value with history
        """
        return Database(remote=self._remote, tx_min=self._tx_min, with_datoms=self._with_datoms, full_history=True)

    def as_if(self, facts: Facts) -> tuple['Database', 'Database', list[Datom], dict[str,int]]:
        """returns a database value with added facts that are not transacted to a remote database, 
        but only visible locally (this function is called `with` in Datomic)

        This function has the same interface as Connection.transact"""
        db_before = Database(remote=self._remote, tx_min=-1, with_datoms=self._with_datoms, full_history=False)
        tx_data, temp_ids = db_before._transaction_data(facts)
        db_before._validate_transaction(tx_data)
        with_datoms = self._with_datoms.append(tx_data)
        db_after = Database(remote=self._remote, tx_min=-1, with_datoms=with_datoms, full_history=False)
        return db_before, db_after, tx_data, temp_ids

    def _validate_transaction(self, tx_data):
        lst = []
        db = self._applicative_copy()
        for datom in tx_data:
            # given db, check if is adding datom is a valid operation
            attr_def = db._validate_datom(datom)
            # apply datom to db
            db._apply_datom(datom)
            value_mongo = attr_def.value_type.mongo_encode(datom)
            lst.append(value_mongo)
        return lst
    
    def _validate_datom(self, datom):
        # validate that a single new datom is valid if added at the end of the current db
        attr_def = self._get_attr_def(datom.a)
        attr_def.validate_value(datom.v)
        attr_def.validate_ref(datom.v, self)
        data = self.get(datom.e)
        existing_value = None if datom.a not in data else data[datom.a]
        attr_def.validate_cardinality(datom.e, datom.v, datom.op, existing_value)
        attr_def.validate_uniqueness(datom.v, datom.op, self)
        return attr_def

    def _applicative_copy(self):
        # return a new database that can be subsequently filled using the _apply_datom function
        # this is useful for fast validation of a new set of facts
        d = self._with_datoms.append([])
        return Database(remote=self._remote, tx_min=-1, with_datoms=d, full_history=False)

    def _apply_datom(self, datom):
        # apply given datom to the database in the last local transaction (in-place)
        # this is useful for fast validation of a new set of facts
        self._with_datoms.append_fact(datom)

        # if datom is about a defined attribute (an entity that has 'db/ident'), invalidate the attr_def_cache of that entity
        for a, (e, _) in self._attr_def_cache.items():
            if datom.e == e:
                del self._attr_def_cache[a]
                break

        # if datom is about an attribute in attribute index -> update this index
        if datom.a in self._attr_index:
            f = self._attr_index[datom.a].facts
            f.append(datom)
            self._attr_index[datom.a] = Index(f)

        # if datom is about an attribute in attribute/value index -> update this index
        if datom.a in self._attr_val_index:
            if datom.v in self._attr_val_index[datom.a]:
                f = self._attr_val_index[datom.a][datom.v].facts
                f.append(datom)
                self._attr_val_index[datom.a][datom.v] = Index(f)
            else:
                if datom.a in self._attr_val_index_complete:
                    self._attr_val_index[datom.a][datom.v] = Index([datom])

        # if datom is about an entity in entity index -> update this index
        if datom.e in self._entity_index:
            f = self._entity_index[datom.e].facts
            f.append(datom)
            self._entity_index[datom.e] = Index(f)

    def _get_attr_def(self, name: str) -> Attr:
        builtin_attr = Attr.builtin_dict()
        if name in builtin_attr:
            attr_def = builtin_attr[name]
        else:
            if len(self._attr_def_cache) == 0:
                # init attr cache
                result = self.find({'db/ident': None})
                for e, d in result.items():
                    self._attr_def_cache[d['db/ident']] = (e, Attr.from_dict(d))
            if name in self._attr_def_cache:
                attr_def = self._attr_def_cache[name][1]
            else:
                try:
                    e = self._lookup('db/ident', name)
                except EntityNotFoundError:
                    raise ValueError(f'attribute {name!r} is not defined')
                dct = self.get(e)
                attr_def = Attr.from_dict(dct)
                self._attr_def_cache[name] = (e, attr_def)
        return attr_def

    def query(self, q):
        """complex queries (like in Datomic) [not implemented yet]"""
        # TODO
        raise NotImplementedError()

    def find(self, criteria: dict[str, Any]) -> dict[int, dict[str, Any]]:
        """find entities based on attribute values
        :param criteria: dict of attribute/value pairs that must all be fulfilled, if value is None any attribute value will match
        :return: result dict with key being the entity ID and value being the entity value as dict in the same form as returned by Database.get
        """
        results = {}
        if criteria == {}:
            # return all entities
            for e in self.entities():
                d = self.get(e)
                if d != {}:
                    results[e] = d
        else:
            attr = list(criteria.keys())[0]
            val = criteria[attr]
            candidates = self._find_candidates(attr, val, return_all_attributes=True)
            results = self._filter_candidates(candidates, criteria)
        return results

    def _find_candidates(self, attr, val, return_all_attributes=True):
        candidate_datoms = self._find_attribute_value(attr, val)
        candidate_entities = set(f.e for f in candidate_datoms)
        if return_all_attributes:
            candidate_datoms_all = self._facts_multi_entity(candidate_entities)
        else:
            candidate_datoms_all = candidate_datoms
        candidate_datoms_all_by_e = {}
        for f in candidate_datoms_all:
            if f.e not in candidate_datoms_all_by_e:
                candidate_datoms_all_by_e[f.e] = []
            candidate_datoms_all_by_e[f.e].append(f)
        candidates = {}
        for e in candidate_entities:
            candidates[e] = self._get_from_facts(candidate_datoms_all_by_e[e])
        return candidates
    
    @staticmethod
    def _filter_candidates(candidates, criteria):
        for attr, val in criteria.items():
            new_candidates = {}
            for e, d in candidates.items():
                if attr in d and (val is None or d[attr] == val):
                    new_candidates[e] = d
            candidates = new_candidates
        results = candidates
        return results

    def get(self, entity: Union[int, list]) -> dict[str, Any]:
        """get all current facts of an entity as dict attribute->value
        :param entity: the entity ID as int or lookup ref [attribute, value]
        :return: dict containing current attribute/value pairs, if an attribute has 
        cardinality many, then the value will be a list
        """
        facts = self.facts(entity)
        return self._get_from_facts(facts)

    def _get_from_facts(self, facts: list[Datom]):
        active_facts = self._get_active_facts(facts)
        return self._active_facts_to_dict(active_facts)

    def _get_active_facts(self, facts: list[Datom]):
        active_facts = []
        for f in facts:
            if f.op:
                active_facts.append((f.a, f.v))
            else:
                active_facts.remove((f.a, f.v))
        return active_facts
    
    def _has_active_facts(self, entity):
        facts = self.facts(entity)
        active_facts = self._get_active_facts(facts)
        return len(active_facts) > 0
    
    def _active_facts_to_dict(self, active_facts: list[tuple[str, Any]]):
        d = {}
        for a, v in active_facts:
            attr = self._get_attr_def(a)
            if attr.cardinality == Cardinality.one:
                d[a] = v
            else:
                if a not in d:
                    d[a] = []
                d[a].append(v)
        return d

    def facts(self, entity: Union[int,list]) -> list[Datom]:
        """get all historic facts about an entity as a list of datoms
        :param entity: the entity ID as int or lookup ref [attribute, value]
        :return: list of pydatomic.Datom elements containing all historic facts about the entity
        """
        if isinstance(entity, list):
            entity = self._lookup(entity[0], entity[1])
        if entity < 0:
            return []
        return self._facts_multi_entity([entity])

    def _facts_multi_entity(self, entities: Collection):
        facts = []
        entities = set(entities)
        remaining_entities = set(entities)
        for e in entities:
            if e in self._entity_index:
                facts.extend(self._entity_index[e].facts)
                remaining_entities = remaining_entities - {e}

        if len(remaining_entities) > 0:
            facts_dict = {e: [] for e in remaining_entities}
            if self._remote is not None:
                r = self._remote
                if any(e <= r._e_max for e in remaining_entities):
                    tx_max = r._tx_max
                    lst = r._conn._datoms.find({'e': {'$in': list(remaining_entities)}, 'tx': {'$lte': tx_max}})
                    for d in lst:
                        f = ValueType.mongo_decode(d)
                        facts.append(f)
                        facts_dict[f.e].append(f)

            for e in remaining_entities:
                for f in self._with_datoms.facts_by_entity(e):
                    facts.append(f)
                    facts_dict[e].append(f)

            for e, f in facts_dict.items():
                self._entity_index[e] = Index(f)

        return facts

    def all_facts(self) -> list[Datom]:
        """get all historic facts in the database
        :return: list of pydatomic.Datom elements containing all historic facts in the database
        """
        facts = self._facts_multi_entity(self.entities())
        return sorted(facts)

    def __str__(self):
        h = 'on' if self._full_history else 'off'
        ne = self._get_max_entity_id() + 1
        facts = self.all_facts()
        nf = len(facts)
        nt = len(list(set([f.tx for f in facts])))
        s = f'Database with {ne} entities and {nf} facts from {nt} transactions. History is {h}.\n'
        for f in facts:
            op = '+' if f.op else '-'
            s += f'{op} Tx {f.tx} Fact {f.id}: Entity {f.e} {f.a!r} is {f.v!r}.\n'
        return s

    def __repr__(self):
        return self.__str__()

    def states(self, entity: Union[int, list]) -> dict[int, dict[str, Any]]:
        """get all historic states of an entity as a dict: tx->dict[attribute->value],
        transactions that do not modify the given entity will be omitted
        :param entity: the entity ID as int or lookup ref [attribute, value]
        :return: a dict with keys the transaction IDs and values the state of the entity 
        as of this transaction (as returned by Database.get)
        """
        facts = self.facts(entity)
        states = {}
        active_facts = []
        tx_last = -1
        for f in facts:
            if f.tx != tx_last:
                states[tx_last] = self._active_facts_to_dict(active_facts)
                tx_last = f.tx
            if f.op:
                active_facts.append((f.a, f.v))
            else:
                active_facts.remove((f.a, f.v))
        states[tx_last] = self._active_facts_to_dict(active_facts)
        del states[-1]
        return states


# Helper classes and functions for internal use
class RemoteDatabase:
    """represents a remote database value at a certain point-in-time tx_max, without support for any additional local transactions"""

    def __init__(self, conn: Connection, tx_max: int):
        self._conn = conn
        self._tx_max = tx_max  # inclusive: tx <= tx_max
        f = self._conn._datoms.find_one({'$query': {'tx': {'$lte': tx_max}}, '$orderby': {'e': -1}})
        self._e_max = -1 if f is None else f['e']


class LocalDatoms:
    """represents a list of datoms (along with some caches/indexes for efficient access), not transmitted to a remote database"""

    def __init__(self, facts: list[Datom]) -> None:
        self._datoms: list[Datom] = facts
        self._cache_tx_max = None
        self._cache_e_max = None
        self._cache_id_max = None
        self._cache_a_index = None
        self._cache_av_index = None
        self._cache_e_index = None
    
    def facts(self):
        """iterate over all datoms"""
        for f in self._datoms:
            yield f

    def facts_by_attribute(self, attribute):
        if self._cache_a_index is None:
            d = {}
            for f in self._datoms:
                if f.a not in d:
                    d[f.a] = []
                d[f.a].append(f)
            self._cache_a_index = d
        return self._cache_a_index[attribute] if attribute in self._cache_a_index else []

    def facts_by_attribute_value(self, attribute, value):
        if self._cache_av_index is None:
            d = {}
            for f in self._datoms:
                if f.a not in d:
                    d[f.a] = {}
                if f.v not in d[f.a]:
                    d[f.a][f.v] = []
                d[f.a][f.v].append(f)
            self._cache_av_index = d
        if attribute in self._cache_av_index:
            if value in self._cache_av_index[attribute]:
                return self._cache_av_index[attribute][value]
            else:
                return []
        else:
            return []

    def facts_by_entity(self, entity):
        if self._cache_e_index is None:
            d = {}
            for f in self._datoms:
                if f.e not in d:
                    d[f.e] = []
                d[f.e].append(f)
            self._cache_e_index = d
        return self._cache_e_index[entity] if entity in self._cache_e_index else []

    def __len__(self):
        return len(self._datoms)

    def tx_max(self):
        if self._cache_tx_max is None:
            self._cache_tx_max = max_default((f.tx for f in self._datoms), -1)
        return self._cache_tx_max

    def append(self, facts: list[Datom]):
        """return a new LocalDatoms object with a set of appended facts"""
        f = self._datoms.copy()
        f.extend(facts)
        return LocalDatoms(f)

    def append_fact(self, datom: Datom):
        """append a single fact in-place, mutating the object and updating all caches"""
        self._datoms.append(datom)
        if self._cache_tx_max is not None and datom.tx > self._cache_tx_max:
            self._cache_tx_max = datom.tx
        if self._cache_e_max is not None and datom.e > self._cache_e_max:
            self._cache_e_max = datom.e
        if self._cache_id_max is not None and datom.id > self._cache_id_max:
            self._cache_id_max = datom.id
        if self._cache_a_index is not None:
            if datom.a not in self._cache_a_index:
                self._cache_a_index[datom.a] = []
            self._cache_a_index[datom.a].append(datom)
        if self._cache_av_index is not None:
            if datom.a not in self._cache_av_index:
                self._cache_av_index[datom.a] = {}
            if datom.v not in self._cache_av_index[datom.a]:
                self._cache_av_index[datom.a][datom.v] = []
            self._cache_av_index[datom.a][datom.v].append(datom)
        if self._cache_e_index is not None:
            if datom.e not in self._cache_e_index:
                self._cache_e_index[datom.e] = []
            self._cache_e_index[datom.e].append(datom)

    def as_of(self, tx_id: int):
        """return a new LocalDatoms object with only datoms d where d.tx <= tx_id"""
        facts = []
        for f in self._datoms:
            if f.tx <= tx_id:
                facts.append(f)
        return LocalDatoms(facts)
    
    def max_key(self, key):
        if key not in ['e', '_id']:
            raise ValueError(f'unsupported key: {key}')
        if self._cache_e_max is None or self._cache_id_max is None:
            self._cache_e_max = max_default((f.e for f in self._datoms), -1)
            self._cache_id_max = max_default((f.id for f in self._datoms), -1)
        v = {'e': self._cache_e_max, '_id': self._cache_id_max}
        return v[key]


class Index:
    def __init__(self, facts):
        self.facts = facts


def max_default(seq, default_value):
    m = default_value
    for v in seq:
        if v > m:
            m = v
    return m
