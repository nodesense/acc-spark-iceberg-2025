```

show databases;


CREATE DATABASE IF NOT EXISTS tpchiveraw;
USE tpchiveraw;



CREATE EXTERNAL TABLE IF NOT EXISTS tpchiveraw.customers (
    c_custkey INT,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DOUBLE,
    c_mktsegment STRING,
    c_comment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://tpch-source/customers'
TBLPROPERTIES ("skip.header.line.count"="1");


SHOW TABLES;


SELECT * FROM tpchiveraw.customers LIMIT 10;


----

CREATE EXTERNAL TABLE IF NOT EXISTS tpchiveraw.nations (
    n_nationkey INT,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://tpch-source/nations'
TBLPROPERTIES ("skip.header.line.count"="1");


SELECT * FROM tpchiveraw.nations LIMIT 10;


---

SELECT 
    n.n_name AS nation_name, 
    COUNT(c.c_custkey) AS customer_count
FROM tpchiveraw.nations n
JOIN tpchiveraw.customer c 
    ON n.n_nationkey = c.c_nationkey
GROUP BY n.n_name
ORDER BY customer_count DESC;




---

CREATE EXTERNAL TABLE IF NOT EXISTS tpchiveraw.orders (
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DOUBLE,
    o_orderdate STRING,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://tpch-source/orders'
TBLPROPERTIES ("skip.header.line.count"="1");


---

SELECT 
    n.n_name AS nation_name, 
    COUNT(DISTINCT o.o_custkey) AS customers_with_orders
FROM tpchiveraw.nations n
JOIN tpchiveraw.customer c 
    ON n.n_nationkey = c.c_nationkey
JOIN tpchiveraw.orders o 
    ON c.c_custkey = o.o_custkey
GROUP BY n.n_name
ORDER BY customers_with_orders DESC;

```
