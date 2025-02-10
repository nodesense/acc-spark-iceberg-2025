
ensure --no-address removed

```
gcloud dataproc clusters create cluster-4aaa --enable-component-gateway --region us-central1  --single-node --master-machine-type n2-standard-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 50 --image-version 2.2-debian12 --optional-components JUPYTER --project iceberg-feb-20205 --tags=dataproc-cluster
```


add public ip

```
gcloud compute instances add-access-config cluster-4aaa-m \
    --zone=us-central1-c

```

To see public ip
```
gcloud compute instances list --filter="name:cluster-4aaa-m" 
```

or 

```
gcloud compute instances list --filter="name:cluster-4aaa-m" --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```

thrift port access [default port is 10000, we don't use that here as we have hive server running]

```
gcloud compute firewall-rules create allow-thrift \
    --allow=tcp:10111 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=dataproc-cluster \
    --network=default
```

Start server

```

spark-submit \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --jars gs://tpch-source/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,/usr/lib/spark/jars/spark-3.4-bigquery-0.34.0.jar \
  --conf spark.hadoop.hive.server2.thrift.port=10111 \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=gs://gks-tpch/iceberg_warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.app.name=IcebergJDBCServer \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  --conf spark.hadoop.hive.server2.logging.operation.log.location=/tmp/gks-thrift-server3
```

beeline jdbc client

```

beeline -u "jdbc:hive2://<<ip>>:10111/default"
```

Big Query

```

CREATE TEMPORARY VIEW bq_movies
USING bigquery
OPTIONS (
  table "iceberg-feb-20205.movies.moviest2"
);

SELECT * FROM bq_movies LIMIT 10;
```
