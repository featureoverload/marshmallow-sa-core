from marshmallow_sa_core import JSONTableSchema

import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL

from sqlalchemy import Table, Column, Integer, String, UniqueConstraint


class ConstraintTest(fixtures.TestBase, AssertsExecutionResults):
    __dialect__ = "default"
    __backend__ = True

    def test_unique_constraint_create(self):
        articles_json_table = {
            'name': 'articles',
            'title': '文章',
            'fields': [
                {'name': 'id', 'type': 'int', 'constraints': {'required': True}},
                {'name': 'title', 'type': 'str', 'constraints': {'required': True, 'unique': True}},
                {'name': 'content', 'type': 'str'},
            ],
            'primaryKey': ['id']
        }
        articles_table: sa.Table = JSONTableSchema().load(articles_json_table)

        metadata = articles_table.metadata
        self.assert_sql_execution(
            testing.db,
            lambda: metadata.create_all(testing.db, checkfirst=False),
            AllOf(
                CompiledSQL(
                    "CREATE TABLE articles ("
                    "id INTEGER NOT NULL, "
                    "title VARCHAR NOT NULL, "
                    "content VARCHAR, "
                    "PRIMARY KEY (id), "
                    "UNIQUE (title)"
                    ")"
                ),
            ),
        )

    @testing.provide_metadata
    def test_unique_constraint_create_with_provide_metadata(self):
        metadata = self.metadata

        posts_json_table = {
            'name': 'posts',
            'title': '文章',
            'fields': [
                {'name': 'id', 'type': 'int', 'constraints': {'required': True}},
                {'name': 'title', 'type': 'str', 'constraints': {'required': True, 'unique': True}},
                {'name': 'content', 'type': 'str'},
            ],
            'primaryKey': ['id']
        }
        schema = JSONTableSchema(context={'metadata': metadata})
        posts_table: sa.Table = schema.load(posts_json_table)

        # TODO: UniqueConstraint on table level
        bar = Table(
            "bar",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", String(30)),
            Column("value2", String(30)),
            UniqueConstraint("value", "value2", name="uix1"),
        )

        self.assert_sql_execution(
            testing.db,
            lambda: metadata.create_all(testing.db, checkfirst=False),
            AllOf(
                CompiledSQL(
                    "CREATE TABLE posts ("
                    "id INTEGER NOT NULL, "
                    "title VARCHAR NOT NULL, "
                    "content VARCHAR, "
                    "PRIMARY KEY (id), "
                    "UNIQUE (title)"
                    ")"
                ),
                CompiledSQL(
                    "CREATE TABLE bar ("
                    "id INTEGER NOT NULL, "
                    "value VARCHAR(30), "
                    "value2 VARCHAR(30), "
                    "PRIMARY KEY (id), "
                    "CONSTRAINT uix1 UNIQUE (value, value2)"
                    ")"
                ),
            ),
        )


class CheckConstrainTest(fixtures.TestBase, AssertsExecutionResults):
    __dialect__ = "default"

    @testing.provide_metadata
    def test_definition_with_constraints(self):
        metadata = self.metadata

        json_table_1 = {
            'name': 'comments_1',
            'fields': [
                {'name': 'entry_id', 'type': 'int', 'constraints': {'required': True}},
                {'name': 'comment', 'type': 'str', 'constraints': {'required': True, 'minLength': 1}},
                {'name': 'like', 'type': 'int', 'constraints': {'minimum': 1}},
            ],
            'primaryKey': ['entry_id']
        }

        json_table_2 = {
            'name': 'comments_2',
            'fields': [
                {'name': 'entry_id', 'type': 'int', 'constraints': {'required': True}},
                {'name': 'comment', 'type': 'str', 'constraints': {'required': True, 'maxLength': 200}},
                {'name': 'like', 'type': 'int', 'constraints': {'minimum': 1}},
            ],
            'primaryKey': ['entry_id']
        }

        json_table_3 = {
            'name': 'comments_3',
            'title': '评论',
            'fields': [
                {'name': 'entry_id', 'type': 'int', 'constraints': {'required': True}},
                {'name': 'comment', 'type': 'str', 'constraints': {'required': True}},
                {'name': 'like', 'type': 'int', 'constraints': {'maximum': 100}},
            ],
            'primaryKey': ['entry_id']
        }

        schema = JSONTableSchema(context={'metadata': metadata})
        for json_table in (json_table_1, json_table_2, json_table_3):
            schema.load(json_table)

        self.assert_sql_execution(
            testing.db,
            lambda: metadata.create_all(testing.db, checkfirst=False),
            AllOf(
                CompiledSQL(
                    'CREATE TABLE comments_1 ('
                    'entry_id INTEGER NOT NULL, '
                    'comment VARCHAR NOT NULL CHECK (LENGTH("comment") >= 1), '
                    '"like" INTEGER CHECK ("like" >= 1.0), '
                    'PRIMARY KEY (entry_id)'
                    ')'
                ),
                CompiledSQL(
                    'CREATE TABLE comments_2 ('
                    'entry_id INTEGER NOT NULL, '
                    'comment VARCHAR NOT NULL CHECK (LENGTH("comment") <= 200), '
                    '"like" INTEGER CHECK ("like" >= 1.0), '
                    'PRIMARY KEY (entry_id)'
                    ')'
                ),
                CompiledSQL(
                    'CREATE TABLE comments_3 ('
                    'entry_id INTEGER NOT NULL, '
                    'comment VARCHAR NOT NULL, '
                    '"like" INTEGER CHECK ("like" <= 100.0), '
                    'PRIMARY KEY (entry_id)'
                    ')'
                ),
            ),
        )
