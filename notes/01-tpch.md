```
nations:

n_nationkey, n_name, n_regionkey, n_comment

regions:

r_regionkey, r_name, r_comment

orders:

o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment

suppliers:

s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment

lineitems:

l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment

customers:

c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment

parts:

p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment

part-suppliers:

ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment

```

SSH into master

```
hive
```

```
CREATE DATABASE IF NOT EXISTS tpchive;
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS tpchive.regions (
    r_regionkey INT,
    r_name STRING,
    r_comment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://tpch-source/regions/'
TBLPROPERTIES ('skip.header.line.count'='1');

```

```
SELECT * FROM tpchive.regions LIMIT 10;

```

while hive can understand skip.header.line.count, spark does not. run above query in spark.sql, see the row with column names as data.

```
DROP TABLE IF EXISTS tpchive.regions;
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS tpchive.regions (
    r_regionkey INT,
    r_name STRING,
    r_comment STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "skip.header.line.count"="1"
)
STORED AS TEXTFILE
LOCATION 'gs://tpch-source/regions/';
```
