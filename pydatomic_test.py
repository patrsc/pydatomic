"""Example script to demonstrate usage of pydatomic"""
from pydatomic import Client, Facts

# Create a client
client = Client()
db_name = 'test'
if db_name in client.list_databases():
    client.delete_database(db_name)

client.create_database(db_name)
conn = client.connect('test')
db = conn.db()

# Add facts to the database
f = Facts()
f.add_set(None, {
    'db/ident': 'provider/id',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one',
    'db/unique': 'db.unique/value',
    'db/doc': 'product provider ID'
})
f.add_set(None, {
    'db/ident': 'provider/name',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one',
    'db/doc': 'product provider name'
})
f.add_set(None, {
    'db/ident': 'product/provider',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one',
    'db/doc': 'provider of a product'
})
f.add_set(None, {
    'db/ident': 'product/pid',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one',
    'db/unique': 'db.unique/value',
    'db/doc': 'product ID'
})
f.add_set(None, {
    'db/ident': 'product/exist',
    'db/valueType': 'db.type/boolean',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set(None, {
    'db/ident': 'product/name',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set('apple', {
    'provider/id': 'AAPL',
    'provider/name': 'Apple',
})
f.add_set('nike', {
    'provider/id': 'NIKE',
    'provider/name': 'Nike'
})
f.add_set('p1', {
    'product/provider': 'AAPL',
    'product/pid': 'A2651',
    'product/exist': True,
    'product/name': 'iPhone 14 Pro Max',
})

# Transact the facts
_, db, _, _ = conn.transact(f)

# Add/change facts
f = Facts()
f.remove(['product/pid', 'A2651'], 'product/exist', True)
f.replace(['product/pid', 'A2651'], 'product/name', 
    'iPhone 14 Pro Max', 'iPhone 14 Pro Max (US)')

f.add_set(None, {
    'product/provider': 'NIKE',
    'product/pid': '1234',
    'product/exist': True,
})
f.add_set(None, {
    'db/ident': 'tx/comment',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set('datomic.tx', {
    'tx/comment': 'fix error'
})

_, db, _, _ = conn.transact(f)

# Add facts and use them in a local 'preview' database (as if)
f = Facts()
f.add_set(None, {
    'db/ident': 'product/new',
    'db/valueType': 'db.type/boolean',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set(['product/pid', 'A2651'], {'product/new': True})
_, db1, _, _ = db.as_if(f)

# Now transact them to the server
conn.transact(f)

# Add more data
f = Facts()
f.add_set(None, {
    'db/ident': 'test/test',
    'db/valueType': 'db.type/string',
    'db/cardinality': 'db.cardinality/one',
    'db/unique': 'db.unique/value'
})
f.add_set(['product/pid', 'A2651'], {'test/test': 'asdf'})
f.add_set(['product/pid', 'A2651'], {'product/exist': False})
_, db2, _, _ = db1.as_if(f)

conn.transact(f)

# Show different database values
print('db:')
print(db)

print('db1:')
facts = db1.facts(['product/pid', 'A2651'])
for f in facts:
    print(f)

print('db2:')
facts = db2.facts(['product/pid', 'A2651'])
for f in facts:
    print(f)
print(db2.get(['product/pid', 'A2651']))

f = Facts()
f.remove(['product/pid', 'A2651'], 'product/new', True)
_, db3, _, _ = db2.as_if(f)
print(db3.get(['product/pid', 'A2651']))

print(conn.db().get(['product/pid', 'A2651']))

# Show information at different points in time
print('product A2651:')
print(conn.db().as_of(0).get(['product/pid', 'A2651']))
print(conn.db().as_of(10).get(['product/pid', 'A2651']))
print(conn.db().as_of(13).get(['product/pid', 'A2651']))
print(conn.db().as_of(15).get(['product/pid', 'A2651']))

# Add more facts with different data types
f = Facts()
f.add_set(None, {
    'db/ident': 'test/number',
    'db/valueType': 'db.type/double',
    'db/cardinality': 'db.cardinality/many'
})
f.add_set(None, {
    'db/ident': 'test.adf.asdf34-4_.r__/int',
    'db/valueType': 'db.type/long',
    'db/cardinality': 'db.cardinality/one',
    'db/unique': 'db.unique/value'
})
f.add_set(None, {
    'db/ident': 'test/uuid',
    'db/valueType': 'db.type/uuid',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set(None, {
    'db/ident': 'test/uri',
    'db/valueType': 'db.type/uri',
    'db/cardinality': 'db.cardinality/one',
    'db/doc': 'URI test'
})
f.add_set(None, {
    'db/ident': 'test/ref',
    'db/valueType': 'db.type/ref',
    'db/cardinality': 'db.cardinality/one'
})
f.add_set(None, {'test/number': 0.0})
f.add_set(None, {'test/number': 1.0})
f.add_set('new', {'test/number': 3.14})
f.add('new', 'test/number', 3.2)
f.add('new', 'test/number', 3.3)
f.remove('new', 'test/number', 3.2)
f.add_set(None, {'test/number': float('nan')})
f.add_set(None, {'test/number': float('inf')})
f.add_set(None, {'test/number': float('-inf')})
f.add_set(None, {'test.adf.asdf34-4_.r__/int': 2})
f.add_set(None, {'test/uuid': '5338d5e4-6f3e-45fe-8af5-e2d96213b300'})
f.add_set(None, {'test/uri': 'http://example.com'})
f.add_set(None, {'test/test': 'asdf1'})
conn.transact(f)

f = Facts()
f.add_set(None, {'test/ref': 31})

conn.transact(f)

# Get all the states an entity had over time
db = conn.db()

states = db.states(['product/pid', 'A2651'])
for tx, state in states.items():
    print(tx, state)

# Find entity based on attribute values
results = db.find({
    'db/cardinality': 'db.cardinality/one',
    'db/valueType': 'db.type/string',
    'db/doc': None
})
print('find results:')
for e, v in results.items():
    print(e, v)

# Close the client
client.close()
