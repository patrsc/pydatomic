from pymongo import MongoClient, InsertOne
from fastapi.encoders import jsonable_encoder
from typing import Collection, Optional, Union, Any
from .facts import Facts, Datom
from .util import now, datetime, datetime_to_int
from .builtin import Attr, Cardinality, ValueType, Unique
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
        return Connection(self, db_name)

    def close(self):
        """close client and connections to server"""
        self._client.close()

    def __del__(self):
        self.close()


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
        return Database(remote=r, tx_min=-1, with_tx=None, full_history=False)
    
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

    def __init__(self, remote: Optional['RemoteDatabase'] = None, tx_min: int = -1, with_tx: Optional['LocalTransactionList'] = None, full_history=False) -> None:
        """create a new Database object from scratch (use this only without arguments to create a new standalone database
        that is not connected to a server, use Connection.db to get the Database value of a server)
        """
        if with_tx is None:
            with_tx = LocalTransactionList([])
        self._remote = remote  # if None, this is a virtual db assuming an empty remote database
        self._tx_min = tx_min  # exclusive: tx > tx_min
        self._with_tx = with_tx  # transaction ids monotonically increasing
        self._full_history = full_history
        self._attr_def_cache = {}  # cache of attributes for performance
        self._attr_index = {}  # cached datoms grouped by attribute: {a->AttributeIndex([datom where datom.a==a])}
        self._entity_index = {}  # cached datoms grouped by entity: {e->EntityIndex([datom where datom.e==e])}  TODO: not yet implemented

    @property
    def _remote_tx_max(self):
        return -1 if self._remote is None else self._remote._tx_max

    @property
    def _tx_max(self):
        if len(self._with_tx) > 0:
            return self._with_tx[-1].tx_id()
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
        for f in self._with_tx.facts():
            vf = jsonable_encoder(f)[key]
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

    def _get_attr_index(self, attribute: str):
        facts = []
        if self._remote is not None:
            r = self._remote
            tx_max = r._tx_max
            docs = r._conn._datoms.find({'a': attribute, 'tx': {'$lte': tx_max}})
            for d in docs:
                facts.append(ValueType.mongo_decode(d))
        for f in self._with_tx.facts():
            match = f.a == attribute
            if match:
                facts.append(f)
        return AttributeIndex(facts)

    def _find_attribute_value(self, attribute: str, value: Any) -> list[Datom]:
        """return all facts with given (attribute, value) pair, if value is None it finds the attribute with any value"""
        if attribute not in self._attr_index:
            # add attribute to index for fast future attribute lookup
            self._attr_index[attribute] = self._get_attr_index(attribute)
        # use index to filter attribute datoms
        index: AttributeIndex = self._attr_index[attribute]
        if value is None:
            facts = index.facts.copy()
        else:
            facts = index.facts_where(value)
        return facts

    def _lookup(self, attribute, value) -> int:
        """get entity id of an entity with a unique attribute"""
        attr_def = self._get_attr_def(attribute)
        if attr_def.unique not in [Unique.identity, Unique.val]:
            raise ValueError(f'lookup failed because attribute {attribute!r} is not unique')

        facts = self._find_attribute_value(attribute, value)
        candidates = set(f.e for f in facts)
        for e in candidates:
            d = self.get(e)
            if attribute in d and d[attribute] == value:
                return e
        raise EntityNotFoundError(f'no entity found with attribute {attribute!r} set to {value!r}')

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
            indices = [i for i, t in enumerate(self._with_tx._tx) if t.tx_id() == tx_id]
            n = indices[0]
            tx = self._with_tx.range(0, n+1)
        else:
            r = None if self._remote is None else RemoteDatabase(self._remote._conn, tx_id)
            tx = None
        return Database(remote=r, tx_min=-1, with_tx=tx, full_history=False)

    def since(self, tx_id) -> 'Database':
        """returns a database value after a point in time represented by a transaction ID
        :param tx_id: the transaction ID (exclusive)
        :return: the database value with transactions tx > tx_id
        """
        return Database(remote=self._remote, tx_min=tx_id, with_tx=self._with_tx, full_history=False)

    def history(self) -> 'Database':
        """returns a history database value of the given database value
        (currently history has no effect, since complex queries are not implemented yet; 
        to get all historic facts the functions Database.facts and Database.all_facts can be used on both history databases and normal databases)
        :return: the database value with history
        """
        return Database(remote=self._remote, tx_min=self._tx_min, with_tx=self._with_tx, full_history=True)

    def as_if(self, facts: Facts) -> tuple['Database', 'Database', list[Datom], dict[str,int]]:
        """returns a database value with added facts that are not transacted to a remote database, 
        but only visible locally (this function is called `with` in Datomic)

        This function has the same interface as Connection.transact"""
        db_before = Database(remote=self._remote, tx_min=-1, with_tx=self._with_tx, full_history=False)
        tx_data, temp_ids = db_before._transaction_data(facts)
        db_before._validate_transaction(tx_data)
        tx = self._with_tx.append(LocalTransaction(tx_data))
        db_after = Database(remote=self._remote, tx_min=-1, with_tx=tx, full_history=False)
        return db_before, db_after, tx_data, temp_ids

    def _validate_transaction(self, tx_data):
        lst = []
        for i, datom in enumerate(tx_data):
            tx = self._with_tx.append(LocalTransaction(tx_data[0:i]))
            db = Database(remote=self._remote, tx_min=-1, with_tx=tx, full_history=False)
            # given db, check if is adding datom is a valid operation
            attr_def = db._get_attr_def(datom.a)
            attr_def.validate_value(datom.v, db)
            attr_def.validate_cardinality(datom.e, datom.v, db, datom.op)
            attr_def.validate_uniqueness(datom.v, db, datom.op)
            lst.append(attr_def.value_type.mongo_encode(datom))
        return lst

    def _get_attr_def(self, name: str) -> Attr:
        builtin_attr = Attr.builtin_dict()
        if name in builtin_attr:
            attr_def = builtin_attr[name]
        else:
            if len(self._attr_def_cache) == 0:
                # init attr cache
                result = self.find({'db/ident': None})
                for _, d in result.items():
                    self._attr_def_cache[d['db/ident']] = Attr.from_dict(d)
            if name in self._attr_def_cache:
                attr_def = self._attr_def_cache[name]
            else:
                try:
                    e = self._lookup('db/ident', name)
                except EntityNotFoundError:
                    raise ValueError(f'attribute {name!r} is not defined')
                dct = self.get(e)
                attr_def = Attr.from_dict(dct)
                self._attr_def_cache[name] = attr_def
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
            candidate_datoms = self._find_attribute_value(attr, val)
            candidate_entities = [f.e for f in candidate_datoms]
            candidate_datoms_all = self._facts_multi_entity(candidate_entities)
            candidates = {}
            for e in candidate_entities:
                entity_datoms = [f for f in candidate_datoms_all if f.e == e]
                candidates[e] = self._get_from_facts(entity_datoms)
            results = self._filter_candidates(candidates, criteria)
        return results
    
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
        active_facts = []
        for f in facts:
            if f.op:
                active_facts.append((f.a, f.v))
            else:
                active_facts.remove((f.a, f.v))
        return self._active_facts_to_dict(active_facts)
    
    def _active_facts_to_dict(self, active_facts: list[tuple[str, Any]]):
        d = {}
        for f in active_facts:
            a = f[0]
            v = f[1]
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
        if len(entities) == 0:
            return []
        facts = []
        if self._remote is not None:
            r = self._remote
            tx_max = r._tx_max
            lst = r._conn._datoms.find({'e': {'$in': list(entities)}, 'tx': {'$lte': tx_max}})
            for d in lst:
                facts.append(ValueType.mongo_decode(d))

        for f in self._with_tx.facts():
            if f.e in entities:
                facts.append(f)
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


class LocalTransaction:
    """represents a local transaction, not transmitted to a remote database"""

    def __init__(self, tx_data: list[Datom]) -> None:
        self._datoms: list[Datom] = tx_data
    
    def tx_id(self):
        return self._datoms[0].tx

    def facts(self):
        """iterate over datoms"""
        for f in self._datoms:
            yield f
    
    def __len__(self):
        return len(self._datoms)


class LocalTransactionList:
    """represents a local transactions"""

    def __init__(self, tx: list[LocalTransaction]) -> None:
        self._tx = tx

    def transactions(self):
        """iterate over transactions"""
        for t in self._tx:
            yield t

    def facts(self):
        """iterate over datoms"""
        for t in self._tx:
            for f in t.facts():
                yield f

    def append(self, tx: LocalTransaction):
        """return new LocalTransactionList object with an appended LocalTransaction"""
        tx_list = self._tx.copy()
        tx_list.append(tx)
        return LocalTransactionList(tx_list)

    def __len__(self):
        return len(self._tx)

    def __getitem__(self, index: int) -> LocalTransaction:
        return self._tx[index]

    def range(self, start: int, stop: int) -> 'LocalTransactionList':
        return LocalTransactionList(self._tx[start:stop])


class AttributeIndex:
    def __init__(self, facts):
        self.facts = facts  # all facts about a single attribute
        self.values = {}  # dict of values mapping to indices in facts where the value occurs
        for i, f in enumerate(facts):
            if f.v not in self.values:
                self.values[f.v] = []
            self.values[f.v].append(i)

    def facts_where(self, value):
        """return the facts where f.v == value"""
        if value in self.values:
            facts = [self.facts[i] for i in self.values[value]]
        else:
            facts = []
        return facts
