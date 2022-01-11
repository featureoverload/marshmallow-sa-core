from enum import Enum


# override the Enum's default stringify method
# refer: https://stackoverflow.com/questions/24487405
Enum.__str__ = lambda self: str(self.value)


class DBColumnType(Enum):
  bool = 'bool'
  datetime = 'datetime'
  date = 'date'
  float = 'float'
  int = 'int'
  json = 'json'
  str = 'str'
  time = 'time'
  bit = 'bit'
  bigint = 'bigint'
  #: Unix Timestamp(not pandas.Timestamp), represent seconds count from 1970/1/1 00:00:00.
  timestamp = 'timestamp'
