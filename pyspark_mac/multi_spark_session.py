#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""this code shows how SparkSession work if some sessions exist."""

from pyspark.sql import SparkSession

from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def main():
    """main function."""
    spark1 = SparkSession.builder.appName('GlobalTemp_1').getOrCreate()
    spark2 = SparkSession.builder.appName('GlobalTemp_2').getOrCreate()
    exit()

    # define coordinate table
    x_schema = StructType([
        StructField('x', IntegerType(), True),
    ])
    x1 = [[1]]
    x2 = [[2]]
    df_1 = spark1.createDataFrame(x1, schema=x_schema)
    df_1.show()
    df_2 = spark2.createDataFrame(x2, schema=x_schema)
    df_2.show()

    df_x_on_1 = df_1.union(df_2)
    df_x_on_2 = df_2.union(df_1)
    df_x_on_1.show()
    df_x_on_2.show()
    spark1.stop()
    # df_x_on_1.show()  # error will occur.
    df_x_on_2.show()
    spark2.stop()

if __name__ == '__main__':
    main()
