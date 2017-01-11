#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""sample code for spark."""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

def main():
    """main function."""
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    hw_schema = StructType([
        StructField('hello', StringType(), True),
    ])
    hello = [['world']]
    df = spark.createDataFrame(hello, schema=hw_schema)
    df.show()
    spark.stop()

if __name__ == '__main__':
    main()
