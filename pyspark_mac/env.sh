export SPARK_HOME=/usr/local/Cellar/apache-spark
export SPARK_HOME=$SPARK_HOME/`ls $SPARK_HOME | xargs -n 1`
export SPARK_HOME=$SPARK_HOME/libexec
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:`ls $SPARK_HOME/python/lib/py4j-*.zip`
