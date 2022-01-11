"""Table Schema

ref: https://specs.frictionlessdata.io/table-schema/#types-and-formats

- load to SQLAlchemy Table.
- dump SQLAlchemy Table to jsonable table data.
"""

from marshmallow import Schema
from marshmallow import fields as ma_fields
from marshmallow import post_load
from marshmallow import pre_dump
from marshmallow.validate import Length
from marshmallow_enum import EnumField

from sqlalchemy import CheckConstraint as Check
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
            'args': constraints
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

    def _checks_in_constraints(self, constraints, field_name):
        checks = []
        for name, value in constraints.items():
            if name == 'minLength':
                checks.append(Check('LENGTH("%s") >= %s' % (field_name, value)))
            elif name == 'maxLength':
                checks.append(Check('LENGTH("%s") <= %s' % (field_name, value)))
            elif name == 'minimum':
                checks.append(Check('"%s" >= %s' % (field_name, value)))
            elif name == 'maximum':
                checks.append(Check('"%s" <= %s' % (field_name, value)))
            elif name == 'pattern':
                raise NotImplementedError('not support yet')
                if self.__dialect in ['postgresql']:
                    checks.append(Check('"%s" ~ \'%s\'' % (field_name, value)))
                else:
                    checks.append(Check('"%s" REGEXP \'%s\'' % (field_name, value)))
            elif name == 'enum':
                raise NotImplementedError('not support yet')
                column_type = sa.Enum(*value, name='%s_%s_enum' % (table_name, field.name))
            else:
                raise ValueError(f'unknown "{name}"')
        return checks

    @post_load
    def create_object(self, data, **_) -> Column:
        if 'constraints' in data:
            kwargs = data.pop('constraints')
            kwargs['checks'] = self._checks_in_constraints(kwargs.pop('args'), data['name'])
            data.update(kwargs)
        if 'description' in data:
            data['comment'] = data.pop('description')
        return SAColumnSchema().load(data)

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
