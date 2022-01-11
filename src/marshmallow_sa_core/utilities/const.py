from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Time
from sqlalchemy.dialects.postgresql import JSONB

from marshmallow_sa_core.utilities.enum import DBColumnType


COLUMNTYPE_TO_SA_TYPE_MAPPING = {
  DBColumnType.bool: Boolean,
  DBColumnType.str: String,
  DBColumnType.int: Integer,
  DBColumnType.float: Float,
  DBColumnType.datetime: DateTime,
  DBColumnType.date: Date,
  DBColumnType.time: Time,
  DBColumnType.json: JSONB,
  DBColumnType.bigint: BigInteger,
}
