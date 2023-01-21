from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.pipeline import Transformer
from pyspark.ml import Pipeline


class IdExtractor(Transformer):
    def __init__(self, output_col='id'):
        self.output_col = output_col

    def this():
        this(Identifiable.randomUID("idextractor"))

    def copy(extra):
        defaultCopy(extra)

    def _transform(self, df):
        return df.withColumn(self.output_col, monotonically_increasing_id())


class YearExtractor(Transformer):
    def __init__(self, input_col, output_col='year'):
        self.input_col = input_col
        self.output_col = output_col

    def this():
        this(Identifiable.randomUID("yearextractor"))

    def copy(extra):
        defaultCopy(extra)

    def _transform(self, df):
        return df.withColumn("year", expr("""
                                                    case when year <=21 then year + 2000
                                                         when year <=100 then year + 1900
                                                         else year
                                                         end
                                               """))


class CastToInteger(Transformer):
    def __init__(self, *args):
        self.args = args

    def this():
        this(Identifiable.randomUID("casttointeger"))

    def copy(extra):
        defaultCopy(extra)

    def _transform(self, df):
        for arg in self.args:
            df = df.withColumn(arg, col(arg).cast(IntegerType()))
        return df


class GenerateDob(Transformer):
    def __init__(self, day, month, year):
        self.day = day
        self.month = month
        self.year = year

    def this():
        this(Identifiable.randomUID("dobextractor"))

    def copy(extra):
        defaultCopy(extra)

    def _transform(self, df):
        return df.withColumn("DOB", concat(col(self.day), lit('/'), col(self.month), lit('/'), col(self.year)))


class DropDuplicates(Transformer):
    def __init__(self, input_cols):
        self.input_cols = input_cols

    def this():
        this(Identifiable.randomUID("dobextractor"))

    def copy(extra):
        defaultCopy(extra)

    def _transform(self, df):
        return df.dropDuplicates(self.input_cols)
