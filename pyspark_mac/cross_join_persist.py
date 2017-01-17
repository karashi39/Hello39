#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""sample code of cross join for pyspark."""

import sys

from pyspark import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)


def main(argv):
    """main function."""
    spark = SparkSession.builder.appName('CrossJoin').getOrCreate()
    spark.conf.set('spark.sql.crossJoin.enabled', True)
    band_schema = StructType([
        StructField('band', StringType(), True),
        StructField('part', StringType(), True),
        StructField('member', StringType(), True),
    ])
    df_all = spark.createDataFrame([], schema=band_schema)
    input_files = argv[1:]
    for input_file in input_files:
        df = spark.read.csv(input_file, schema=band_schema, header='true')
        df_all = df_all.union(df)
    mst_band = df_all.select(['band']).distinct()
    mst_band.persist(storageLevel=MEMORY_AND_DISK)
    mst_part = df_all.select(['part']).distinct()
    mst_part.persist(storageLevel=MEMORY_AND_DISK)
    df_cross = mst_band.join(mst_part)
    df_cross.show()
    spark.stop()

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print 'should be 1 or more argument.'
        exit()
    main(sys.argv)
