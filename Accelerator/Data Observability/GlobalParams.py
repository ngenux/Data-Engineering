from enum import Enum


class GlobalParams(Enum):
    NUMERIC_TYPES = ['int8', 'int16', 'int32',
                     'int64', 'float16', 'float32', 'float64']
    CATEGORY_TYPES = ['bool', 'category']
    DATETIME_TYPES = ['datetime64[ns]', 'timedelta[ns]']




