#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""sample code for spark."""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def main():
    """main function."""
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    # list of records source including None data.
    hello = [['world', 'hello'],['hello', None],['hello', 'world']]

    # create DataFrame with no NOTNULL column from list including None data.
    nullable_schema = StructType([
        StructField('nullable 1', StringType(), True),
        StructField('nullable 2', StringType(), True),
    ])
    df = spark.createDataFrame(hello, schema=nullable_schema)
    df.show()
    """
    result like this.

    +----------+----------+
    |nullable 1|nullable 2|
    +----------+----------+
    |     world|     hello|
    |     hello|      null|
    |     hello|     world|
    +----------+----------+
    """

    # create DataFrame with NOTNULL column from list include None data.
    not_nullable_schema = StructType([
        StructField('nullable 1', StringType(), True),
        StructField('not nullable', StringType(), False),
    ])
    df = spark.createDataFrame(hello, schema=not_nullable_schema)
    df.show()
    """
    raise Value Error as is script execution. 
    like this.

    ValueError: This field is not nullable, but got None.
    """

    spark.stop()

if __name__ == '__main__':
    main()
