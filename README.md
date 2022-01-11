# marshmallow-sa-core

SQLAlchemy-core integration with marshmallow (de)serialization library.


## Declare table

`table.json`:

```json
{
  "name": "market",
  "fields": [
    {"name": "id", "type": "int", "constraints": {"required": true}},
    {"name": "name", "type": "str", "constraints": {"required": true}},
    {"name": "type", "type": "str"},
    {"name": "city", "type": "str"},
    {"name": "tel", "type": "str"},
    {"name": "address", "type": "str", "constraints": {"unique": true}},
    {"name": "lng", "type": "float", "constraints": {"minimum": -180, "maximum": 180}},
    {"name": "lat", "type": "float", "constraints": {"minimum": -90, "maximum": 90}},
    {"name": "create_time", "type": "datetime"},
    {"name": "update_time", "type": "datetime"}
  ],
  "primaryKey": ["id"]
}
```

## Generate(Deserialize) SQLAlchemy Table

```python
>>> import json
>>> with open('table.json', 'r') as fp:
        table_definition = json.load(fp)
>>> from marshmallow_sa_core import JSONTableSchema
>>> schema = JSONTableSchema()
>>> table = schema.load(table_definition)
>>> table
Table('market',
      MetaData(),
      Column('id', Integer(), table=<market>, primary_key=True, nullable=False),
      Column('name', String(), table=<market>, nullable=False),
...
```

check DDL of create table:

```python
>>> from sqlalchemy import create_engine
>>> from sqlalchemy.schema import CreateTable
>>> db = create_engine('sqlite:///:memory:')
>>> print(CreateTable(table).compile(db))

CREATE TABLE market (
        id INTEGER NOT NULL, 
        name VARCHAR NOT NULL, 
        type VARCHAR, 
        city VARCHAR, 
        tel VARCHAR, 
        address VARCHAR, 
        lng FLOAT CHECK ("lng" <= 180.0) CHECK ("lng" >= -180.0), 
        lat FLOAT CHECK ("lat" <= 90.0) CHECK ("lat" >= -90.0), 
        create_time DATETIME, 
        update_time DATETIME, 
        PRIMARY KEY (id), 
        UNIQUE (address)
)

```

## Serialize SQLAlchemy Table object

```python
>>> from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime
>>> table = Table(
  'employee',
  MetaData(),
  Column('id', Integer, primary_key=True),
  Column('LastName', String, nullable=False),
  Column('FirstName', String, nullable=False),
  Column('Title', String),
  Column('BirthDate', DateTime),
  Column('HireDate', DateTime),
)
>>> from marshmallow_sa_core import JSONTableSchema
>>> from pprint import pp
>>> pp(JSONTableSchema().dump(table))
{'schema': None,
 'fields': [{'type': 'int',
             'constraints': {'required': True},
             'name': 'id',
             '__version__': '0.0.4'},
            {'type': 'str',
             'constraints': {'required': True},
             'name': 'LastName',
             '__version__': '0.0.4'},
            {'type': 'str',
             'constraints': {'required': True},
             'name': 'FirstName',
             '__version__': '0.0.4'},
            {'type': 'str', 'name': 'Title', '__version__': '0.0.4'},
            {'type': 'datetime', 'name': 'BirthDate', '__version__': '0.0.4'},
            {'type': 'datetime', 'name': 'HireDate', '__version__': '0.0.4'}],
 'name': 'employee',
 'primaryKey': ['id'],
 '__version__': '0.0.4'}
```


## Get it now

```shell
$ pip install marshmallow-sa-core
```

## License

MIT licensed. See the bundled [LICENSE](./LICENSE) file for more details.
