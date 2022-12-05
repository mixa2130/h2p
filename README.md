# h2p - Hadoop to Postgres

Custom implementation of Apache Sqoop, but faster😏

## Why you should use h2p?

* Faster than sqoop
* Works with pg_bouncer based solutions like [Patroni](https://github.com/zalando/patroni)

## Launch

____

~~~shell
echo "run h2p"

PYSPARK_PYTHON={Path to your python3}

SPARK_MAJOR_VERSION=3 spark-submit \
  --master yarn \
  --deploy-mode client \
  --files log4j.properties \
  --py-files export_to_postgres.py,exceptions.py,context.py,logger.py,pg_connector.py \
  --jars "postgresql.jar" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.pyspark.python=$PYSPARK_PYTHON \
  --conf spark.pyspark.driver.python=$PYSPARK_PYTHON \
  --conf spark.driver.memory='10g' \
  --conf spark.executor.memory='5g' \
  --conf spark.driver.maxResultSize='10g' \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=6 \
  --conf spark.default.parallelism=64 \
  --conf spark.sql.shuffle.partitions=100 \
  --conf spark.executor.memoryOverhead='8g' \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.network.timeout='600s' \
  --conf spark.sql.broadcastTimeout=6000 \
  --conf spark.sql.catalogImplementation='hive' \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:log4j.properties" \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:log4j.properties" \
  export_to_postgres.py \
  jceks_provider="" jceks_password_alias="" hive_schema="" hive_table="" \
  postgres_jdbc_url="" tuz_name="{postgres user}" postgres_schema="" postgres_table="" \
  do_crash_on_postgres_connection_error="True" postgres_types="empty" debug="true"

ret_code=$?

echo "ret_code = $ret_code"

~~~