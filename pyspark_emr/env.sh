export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:`ls $SPARK_HOME/python/lib/py4j-0*.zip | xargs -n 1`
