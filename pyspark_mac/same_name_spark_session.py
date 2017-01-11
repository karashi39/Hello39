#!/usr/bin/env python
# -*- encoding:utf-8 -*-
"""
Can SparkSessions that have same appNamecan be defined?
"""

import time

from pyspark.sql import SparkSession


def main():
    """main function."""
    spark1 = SparkSession.builder.appName('SameName').getOrCreate()
    spark2 = SparkSession.builder.appName('SameName').getOrCreate()

    # create two sesseion that have same name.
    d_1= [[1]]
    df_1 = spark1.createDataFrame(d_1)
    df_1.show()

    d_2= [[2]]
    df_2 = spark2.createDataFrame(d_2)
    df_2.show()

    df_union_12 = df_1.union(df_2)
    df_union_12.show()  # normal termination.

    # create another same name session after some process.
    spark3 = SparkSession.builder.appName('SameName').getOrCreate()
    d_3= [[3]]
    df_3 = spark3.createDataFrame(d_3)
    df_3.show()

    df_union_123 = df_union_12.union(df_3)
    df_union_123.show()  # normal termination.

    # use for confirmation for existing session name.
    # time.sleep(120)

    # stop 3rd session and show df_union_12 that has nothing to do session spark3.
    spark3.stop()
    df_1.show()
    df_2.show()
    # df_union_12.show()  # abnormal termination.

    spark1.stop()
    spark2.stop()
    spark3.stop()

if __name__ == '__main__':
    main()
