import sqlalchemy as sa
from sqlalchemy import Column


def assert_sa_table_equal(left, right):
    assert left is not right
    assert isinstance(left, sa.Table)
    assert isinstance(right, sa.Table)
    assert left.name == right.name
    assert left.schema == right.schema
    assert left.columns.keys() == right.columns.keys()
    for key, column in left.columns.items():
        rc = right.columns[key]
        assert_sa_column_equal(column, rc)


def assert_sa_column_equal(left, right):
    assert left.name == right.name
    assert type(left.type) == type(right.type)
    # TODO: Constraints compare
