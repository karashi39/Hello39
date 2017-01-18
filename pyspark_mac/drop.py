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
    hello = [
        ['world', 'hello'],
        ['hello', None],
        ['hello', 'world'],
        [None, 'world'],
    ]

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
    |      null|     world|
    +----------+----------+
    """

    # drop func deletes selected column.
    df.drop('nullable 1').show()
    """
    result like this.

    +----------+
    |nullable 2|
    +----------+
    |     hello|
    |      null|
    |     world|
    |     world|
    +----------+
    """

    # drop func after na deletes records including None.
    df.na.drop().show()
    """
    result like this.

    +----------+----------+
    |nullable 1|nullable 2|
    +----------+----------+
    |     world|     hello|
    |     hello|     world|
    +----------+----------+
    """


    spark.stop()

if __name__ == '__main__':
    main()
