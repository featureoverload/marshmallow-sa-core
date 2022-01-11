import marshmallow_sa_core
from marshmallow_sa_core import JSONTableSchema

from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from sqlalchemy import CheckConstraint
from sqlalchemy.testing import fixtures

import pytest


class SerializationTest(fixtures.TestBase):
    def test_basic(self):
        table = Table(
            'addresses',
            MetaData(schema='scma'),
            Column('id', Integer(), nullable=False, primary_key=True),
            Column('addr', String, nullable=False, unique=True),
            Column('name', String, nullable=False),
            Column('lng', Float, nullable=True),
            Column('lat', Float, ),
        )

        json_table = {
            'name': 'addresses',
            'schema': 'scma',
            'fields': [
                {
                    'name': 'id', 'type': 'int', 'constraints': {'required': True},
                    '__version__': marshmallow_sa_core.__version__,
                },
                {
                    'name': 'addr', 'type': 'str', 'constraints': {'required': True, 'unique': True},
                    '__version__': marshmallow_sa_core.__version__,
                },
                {
                    'name': 'name', 'type': 'str', 'constraints': {'required': True},
                    '__version__': marshmallow_sa_core.__version__,
                },
                {
                    'name': 'lng', 'type': 'float',
                    '__version__': marshmallow_sa_core.__version__,
                },
                {
                    'name': 'lat', 'type': 'float',
                    '__version__': marshmallow_sa_core.__version__,
                },
            ],
            'primaryKey': ['id'],
            '__version__': marshmallow_sa_core.__version__,
        }
        schema = JSONTableSchema()
        serialized = schema.dump(table)
        assert serialized == json_table

    @pytest.mark.xfail(raises=AssertionError)
    def test_serialize_sa_table(self):
        table = Table(
            'posts',
            MetaData(schema='scma'),
            Column('id', Integer(), nullable=False, primary_key=True),
            Column('title', String, nullable=False, unique=True),
            Column('abstract',
                   String,
                   CheckConstraint('LENGTH("comment" <= 200'),
                   nullable=True),
            Column('like',
                   Integer,
                   CheckConstraint("like >= 1")),
        )

        json_table = {
            'name': 'posts',
            'schema': 'scma',
            'fields': [
                {'name': 'id', 'type': 'int', 'constraints': {'required': True},
                 '__version__': marshmallow_sa_core.__version__},
                {'name': 'title', 'type': 'str', 'constraints': {'required': True, 'unique': True},
                 '__version__': marshmallow_sa_core.__version__},
                {'name': 'abstract', 'type': 'str', 'constraints': {'required': False, 'maxLength': 200},
                 '__version__': marshmallow_sa_core.__version__},
                {'name': 'like', 'type': 'int', 'constraints': {'minimum': 1},
                 '__version__': marshmallow_sa_core.__version__},
            ],
            'primaryKey': ['id'],
            '__version__': marshmallow_sa_core.__version__,
        }
        schema = JSONTableSchema()
        serialized = schema.dump(table)
        assert serialized == json_table
