"""Table Schema

ref: https://specs.frictionlessdata.io/table-schema/#types-and-formats

- load to SQLAlchemy Table.
- dump SQLAlchemy Table to jsonable table data.
"""

from typing import Any
from typing import Dict
from marshmallow import Schema
from marshmallow import fields as ma_fields
from marshmallow import post_load
from marshmallow import pre_dump
from marshmallow.validate import Length
from marshmallow_enum import EnumField

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table

from marshmallow_sa_core.utilities.schema import ObjectSchema
from marshmallow_sa_core.utilities.const import COLUMNTYPE_TO_SA_TYPE_MAPPING
from marshmallow_sa_core.utilities.enum import DBColumnType as ColumnTypeEnum

from .ma_sa_core import ColumnSchema as SAColumnSchema
from .ma_sa_core import PrimaryKeyConstraintSchema


class ConstraintsSchema(Schema):
    required = ma_fields.Boolean()
    unique = ma_fields.Boolean()
    minLength = ma_fields.Integer()
    maxLength = ma_fields.Integer()
    minimum = ma_fields.Number()
    maximum = ma_fields.Number()
    pattern = ma_fields.String(metadata={'description': 'not support yet.'})
    enum = ma_fields.String(metadata={'description': 'not support yet.'})

    @post_load
    def constraints_to_sa_column_kwargs(self, constraints: dict, **_) -> dict:
        kwargs = {
            'nullable': not constraints.pop('required', False),
            'unique': constraints.pop('unique', False),
            'checks': constraints,
        }
        return kwargs


class JSONFieldSchema(ObjectSchema):
    """Field Descriptors"""

    class Meta:
        object_class = Column

    name = ma_fields.String(validate=Length(min=1),
                            required=True,
                            metadata={'title': 'name',
                                      'description': "name of field (e.g. column name)"})
    type = EnumField(enum=ColumnTypeEnum, by_value=True,
                     required=True,
                     metadata={'title': 'type',
                               'description': "Indicating the type of this field"})
    description = ma_fields.String(validate=Length(min=1),
                                   required=False,
                                   metadata={'title': 'description',
                                             'description': "A description for the field"})
    title = ma_fields.String(validate=Length(min=1),
                             metadata={'description': "A nicer human readable label or title for this field"})
    format = ma_fields.String(validate=Length(min=1),
                              metadata={'description': "Indicating a format for this field type"})
    constraints = ma_fields.Nested(ConstraintsSchema)

    @post_load
    def create_object(self, data, **_) -> Column:
        self.constraints_as_sa_column_kwargs(data)
        if 'description' in data:
            data['comment'] = data.pop('description')
        return SAColumnSchema().load(data)

    def constraints_as_sa_column_kwargs(self, data: dict) -> None:
        if 'constraints' not in data:
            return

        kwargs = data.pop('constraints')
        kwargs['checks'] = self.checks_to_sa_check_constraints(kwargs.pop('checks'), data['name'])
        data.update(kwargs)

    def checks_to_sa_check_constraints(self, checks: Dict[str, Any], column_name: str):
        sqltexts = {
            'minLength': 'LENGTH("%s") >= %s',
            'maxLength': 'LENGTH("%s") <= %s',
            'minimum': '"%s" >= %s',
            'maximum': '"%s" <= %s',
        }

        sa_check_data: List[Dict[str, str]] = []
        for constraint, value in checks.items():
            sqltext = sqltexts[constraint] % (column_name, value)
            sa_check_data.append({'sqltext': sqltext})
        return sa_check_data

    @pre_dump
    def jsonable_encoder(self, column: Column, **_) -> dict:
        serialized = SAColumnSchema().dump(column)

        constraints = {}
        for check in serialized.pop('checks', []):
            # TODO: support late
            # constraints[''] = ''
            pass
        if not serialized.pop('nullable', True):
            constraints['required'] = True
        if 'unique' in serialized:
            constraints['unique'] = serialized.pop('unique')

        if constraints:
            serialized['constraints'] = constraints
        return serialized


class ReferenceSchema(Schema):
    resource = ma_fields.String(required=True)
    fields = ma_fields.List(ma_fields.String(required=True, validate=Length(min=1)),
                            required=True)


class ForeignKeySchema(Schema):
    fields = ma_fields.List(ma_fields.String(required=True, validate=Length(min=1)),
                            required=True)
    reference = ma_fields.Nested(ReferenceSchema, required=True)


class JSONTableSchema(ObjectSchema):
    class Meta:
        object_class = Table

    name = ma_fields.String(required=True,
                            validate=Length(min=1),
                            metadata={'description': 'the table name'})
    schema = ma_fields.String(required=False,
                              validate=Length(min=1))
    title = ma_fields.String()  # NOTE: useless for now
    fields = ma_fields.List(ma_fields.Nested(JSONFieldSchema))

    # Other Properties
    primaryKey = ma_fields.List(
        ma_fields.String(validate=Length(min=1)),
        validate=Length(min=1))

    @post_load
    def create_object(self, data, **kwargs) -> Table:
        metadata = self.context.get('metadata', MetaData())
        if data.get('schema'):
            metadata.schema = data['schema']

        table = Table(data['name'], metadata)

        for column in data['fields']:
            table.append_column(column)

        if 'primaryKey' in data:
            pk_constraint = PrimaryKeyConstraintSchema().load({
                'columns': data['primaryKey']})
            table.append_constraint(pk_constraint)
        return table

    @pre_dump
    def jsonable_encoder(self, table: Table, **_):
        serialized = {
            'name': table.name,
            'schema': table.schema,
            'fields': [_ for _ in table.columns],
        }
        pk = PrimaryKeyConstraintSchema().dump(table.primary_key)
        serialized['primaryKey'] = pk['columns']
        return serialized
