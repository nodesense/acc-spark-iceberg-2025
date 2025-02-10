from pyspark.sql import SparkSession

# Initialize Spark Session with Iceberg support
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "gs://tpch-source/iceberg") \
    .getOrCreate()

# Create Catalog
spark.sql("""
    CREATE CATALOG iceberg_catalog
    USING HADOOP
    WITH OPTIONS (
        'warehouse'='gs://tpch-source/iceberg'
    )
""")

# List all catalogs
spark.sql("SHOW CATALOGS").show()

# Create Database
spark.sql("CREATE DATABASE iceberg_catalog.tpch")

# List Databases
spark.sql("SHOW DATABASES IN iceberg_catalog").show()

# Create Iceberg Table with tuning properties
spark.sql("""
    CREATE TABLE iceberg_catalog.tpch.sales (
        sale_id BIGINT,
        customer_id BIGINT,
        amount DOUBLE,
        sale_date DATE
    ) USING ICEBERG
    PARTITIONED BY (sale_date)
    TBLPROPERTIES (
        'format-version'='2',
        'write.target-file-size-bytes'='134217728',  -- 128MB target file size
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='copy-on-write',
        'write.parquet.compression-codec'='zstd'
    )
""")

# List Tables in Database
spark.sql("SHOW TABLES IN iceberg_catalog.tpch").show()

# Describe Table
spark.sql("DESCRIBE TABLE iceberg_catalog.tpch.sales").show(truncate=False)

# Insert Sample Data
spark.sql("""
    INSERT INTO iceberg_catalog.tpch.sales VALUES 
    (1, 101, 200.50, '2024-02-01'),
    (2, 102, 450.00, '2024-02-02'),
    (3, 103, 300.75, '2024-02-03')
""")

# Example of Update Operation
spark.sql("""
    UPDATE iceberg_catalog.tpch.sales
    SET amount = amount * 1.1
    WHERE sale_id = 1
""")

# Example of Delete Operation
spark.sql("""
    DELETE FROM iceberg_catalog.tpch.sales
    WHERE sale_id = 3
""")

# Example of Merge (Upsert) Operation
spark.sql("""
    MERGE INTO iceberg_catalog.tpch.sales AS target
    USING (SELECT 2 AS sale_id, 102 AS customer_id, 500.00 AS amount, '2024-02-02' AS sale_date) AS source
    ON target.sale_id = source.sale_id
    WHEN MATCHED THEN UPDATE SET target.amount = source.amount
    WHEN NOT MATCHED THEN INSERT *
""")

# Optimize Table for Performance
spark.sql("""
    CALL iceberg_catalog.tpch.system.rewrite_data_files(
        table => 'iceberg_catalog.tpch.sales',
        options => map(
            'min-input-files', '5',
            'max-input-files', '50',
            'min-file-size-bytes', '52428800',  -- 50MB
            'max-file-size-bytes', '268435456'  -- 256MB
        )
    )
""")

# List Table Snapshots
spark.sql("SELECT * FROM iceberg_catalog.tpch.sales.snapshots").show()

# Rollback to Previous Snapshot
spark.sql("""
    CALL iceberg_catalog.tpch.system.rollback_to_snapshot(
        table => 'iceberg_catalog.tpch.sales',
        snapshot_id => (SELECT max(snapshot_id) FROM iceberg_catalog.tpch.sales.snapshots LIMIT 1)
    )
""")

# Drop Table
spark.sql("DROP TABLE iceberg_catalog.tpch.sales")

# Drop Database
spark.sql("DROP DATABASE iceberg_catalog.tpch")

# Drop Catalog
spark.sql("DROP CATALOG iceberg_catalog")

# Display Table Content
spark.sql("SELECT * FROM iceberg_catalog.tpch.sales").show()
