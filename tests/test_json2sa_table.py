import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy.testing import fixtures

from marshmallow_sa_core import JSONTableSchema
from marshmallow_sa_core.testing import assert_sa_table_equal


class CreateSimpleSATableTest(fixtures.TestBase):
    json_data = {
        "name": "foobar",
        "title": "表名",
        "schema": "scma",
        "fields": [
            {"name": "姓名", "type": "str", },
            {"name": "性别", "type": "str", },
            {"name": "年龄", "type": "int", },
            {"name": "生日", "type": "date", },
            {"name": "身高", "type": "float", },
            {"name": "住址_经度", "type": "float", },
            {"name": "住址_纬度", "type": "float", },
        ]
    }

    expected = sa.Table(
        'foobar',
        sa.MetaData(schema='scma'),
        Column("姓名", sa.String),
        Column("性别", sa.String),
        Column("年龄", sa.Integer),
        Column("生日", sa.Date),
        Column("身高", sa.Float),
        Column("住址_经度", sa.Float),
        Column("住址_纬度", sa.Float),
    )

    def test_default(self):
        table = JSONTableSchema().load(self.json_data)
        assert_sa_table_equal(table, self.expected)

    def test_override_type_mapping(self):
        type_mapping = {
            'str': sa.String,
            'int': sa.Integer,
            'float': sa.Float,
            'date': sa.Date,
        }
        schema = JSONTableSchema(context={'type_mapping': type_mapping})
        table = schema.load(self.json_data)
        assert_sa_table_equal(table, self.expected)
