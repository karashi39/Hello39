#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""create vector from DataFrame on pyspark."""

from pyspark.sql import SparkSession

from pyspark.sql.functions import first
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def main():
    """main function."""
    spark = SparkSession.builder.appName('CreateVector').getOrCreate()
    spark.conf.set('spark.sql.crossJoin.enabled', True)

    # define coordinate table
    coordinate_schema = StructType([
        StructField('x', IntegerType(), True),
        StructField('y', IntegerType(), True),
        StructField('value', IntegerType(), True),
    ])
    coordinate = [
        [0, 0, 0],
        [0, 1, 1],
        [0, 2, 2],
        # [1, 0, 3],
        [1, 1, 4],
        [1, 2, 5],
        [2, 0, 6],
        [2, 1, 7],
        # [2, 2, 8],
    ]
    df_coordinate = spark.createDataFrame(coordinate, schema=coordinate_schema)
    df_coordinate.show()

    # convert coordinate table into vector
    df_vector = df_coordinate.groupBy('x').pivot('y').agg(first('value')).fillna(0)
    df_vector.orderBy('x').show()

if __name__ == '__main__':
    main()
