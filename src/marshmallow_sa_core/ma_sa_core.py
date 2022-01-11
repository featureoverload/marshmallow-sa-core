from typing import TYPE_CHECKING

from marshmallow import Schema
from marshmallow import fields
from marshmallow import post_load
from marshmallow import post_dump
from marshmallow import pre_dump
from marshmallow import validate
from marshmallow.validate import Length
from sqlalchemy import Column
from sqlalchemy import CheckConstraint as Check
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import UniqueConstraint

from marshmallow_sa_core.utilities.const import COLUMNTYPE_TO_SA_TYPE_MAPPING
from marshmallow_sa_core.utilities.schema import ObjectSchema

if TYPE_CHECKING:
    from sqlalchemy.sql.type_api import TypeEngine


class ColumnSchema(Schema):
    name = fields.String(validate=Length(min=1),
                         required=True,
                         metadata={'title': 'name',
                                   'description': "name of field (e.g. column name)"})
    type = fields.Method("get_type", deserialize="load_type",
                         required=True,
                         metadata={'title': 'type',
                                   'description': "Indicating the type of this field"})
    comment = fields.String(validate=Length(min=1),
                            required=False,
                            metadata={'title': 'description',
                                      'description': "A description for the field"})
    checks = fields.List(fields.Raw())
    nullable = fields.Boolean()
    primary_key = fields.Boolean()
    unique = fields.Boolean()

    def get_type(self, obj):
        for type_as_str, sa_col_type in self.context.get('type_mapping', COLUMNTYPE_TO_SA_TYPE_MAPPING).items():
            if isinstance(obj['type'], sa_col_type):
                return type_as_str

    def load_type(self, type_) -> 'TypeEngine':
        return self.context.get('type_mapping', COLUMNTYPE_TO_SA_TYPE_MAPPING)[type_]

    @post_load
    def create_object(self, data, **kw) -> Column:
        # place hold args
        name = data.pop('name')
        type_ = data.pop('type')
        args = data.pop('checks', [])
        return Column(*([name, type_] + args), **data)

    @pre_dump
    def jsonable_encoder(self, column: Column, **_) -> dict:
        serialized = {
            'name': column.name,
            'type': column.type,
            'description': column.comment,
            'checks': list(column.constraints),
            'nullable': column.nullable,
            'primary_key': column.primary_key,
            'unique': column.unique,
        }
        return serialized

    @post_dump
    def remove_none_pair(self, data: dict, **_) -> dict:
        for k, v in data.copy().items():
            if v is None:
                del data[k]
        return data


class PrimaryKeyConstraintSchema(ObjectSchema):
    class Meta:
        object_class = PrimaryKeyConstraint

    columns = fields.List(
        fields.String(validate=Length(min=1)),
        required=True,
        validate=Length(min=1))
    name = fields.String(required=False, validate=Length(min=1))

    @post_load
    def create_object(self, data: dict, **_) -> PrimaryKeyConstraint:
        return PrimaryKeyConstraint(*data['columns'], name=data.get('name', None))

    @pre_dump
    def jsonable_encoder(self, pk_constraint: PrimaryKeyConstraint, **_) -> dict:
        serialized = {
            'columns': [col.name for col in pk_constraint.columns],
        }
        if pk_constraint.name:
            serialized['name'] = pk_constraint.name
        return serialized
