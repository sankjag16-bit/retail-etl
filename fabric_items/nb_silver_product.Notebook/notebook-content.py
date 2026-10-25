# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3d2bcff0-b9b4-4ec2-a636-3594a8ea3927",
# META       "default_lakehouse_name": "lh_retail",
# META       "default_lakehouse_workspace_id": "3c983e34-bf52-4bb4-bf60-9af2c2dae197",
# META       "known_lakehouses": [
# META         {
# META           "id": "3d2bcff0-b9b4-4ec2-a636-3594a8ea3927"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

from pyspark.sql import functions as F

batch_id = ""
entity = "Product"
bronze_table = "bronze_product"
rows_extracted = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if batch_id == "":
    batch_id = spark.table("ctl_watermark").filter(F.col("EntityName") == entity).first()["ActiveBatchID"]
    rows_extracted = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id).count()

landed = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id).count()
assert landed > 0, f"No Bronze rows for batch {batch_id}"
assert landed == int(rows_extracted), f"Bronze has {landed} rows but the copy reported {rows_extracted}"

spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'BRONZE_COMPLETE', RowsExtracted = {landed},
              LastError = NULL, UpdatedAt = current_timestamp() WHERE EntityName = '{entity}'""")
print(batch_id, landed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brand_map = {"hearthco": "HearthCo", "kitchenry": "Kitchenry", "nesthaus": "NestHaus",
             "loomwood": "Loomwood", "brightware": "Brightware"}

brand_expr = F.trim("Brand")
for k, v in brand_map.items():
    brand_expr = F.when(F.lower(F.trim("Brand")) == k, F.lit(v)).otherwise(brand_expr)

b = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id)
clean = (b
    .withColumn("ProductID", F.upper(F.trim("ProductID")))
    .withColumn("ProductName", F.trim("ProductName"))
    .withColumn("Category", F.trim("Category"))
    .withColumn("Subcategory", F.trim("Subcategory"))
    .withColumn("SupplierID", F.upper(F.trim("SupplierID")))
    .withColumn("Brand", brand_expr))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

w = Window.partitionBy("ProductID").orderBy(F.col("ModifiedAt").desc(), F.col("_ingested_at").desc())
incoming = (clean.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
            .select("ProductID", "ProductName", "Brand", "Category", "Subcategory",
                    "SupplierID", "UnitCost", "ModifiedAt", "_batch_id"))
incoming.createOrReplaceTempView("incoming")
print(incoming.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not spark.catalog.tableExists("dim_product"):
    spark.sql("""
        CREATE TABLE dim_product (
            ProductKey BIGINT, ProductID STRING, ProductName STRING, Brand STRING, Category STRING,
            Subcategory STRING, SupplierID STRING, UnitCost DECIMAL(18,2),
            EffectiveFrom TIMESTAMP, EffectiveTo TIMESTAMP, IsCurrent BOOLEAN,
            _batch_id STRING, _silver_updated_at TIMESTAMP) USING DELTA""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

changed = spark.sql("""
    SELECT i.* FROM incoming i
    JOIN dim_product d ON d.ProductID = i.ProductID AND d.IsCurrent = true
    WHERE i.ModifiedAt > d.EffectiveFrom
      AND NOT (d.Brand <=> i.Brand AND d.Category <=> i.Category
               AND d.Subcategory <=> i.Subcategory AND d.SupplierID <=> i.SupplierID)
""")
changed.createOrReplaceTempView("changed")
print("tracked changes:", changed.count())

spark.sql("""
    MERGE INTO dim_product t USING changed s
    ON t.ProductID = s.ProductID AND t.IsCurrent = true
    WHEN MATCHED THEN UPDATE SET t.EffectiveTo = s.ModifiedAt, t.IsCurrent = false,
                                 t._silver_updated_at = current_timestamp()
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

need_insert = spark.sql("""
    SELECT i.* FROM incoming i
    LEFT JOIN (SELECT ProductID FROM dim_product WHERE IsCurrent = true) c ON c.ProductID = i.ProductID
    WHERE c.ProductID IS NULL
""")

start = spark.sql("SELECT COALESCE(MAX(ProductKey), 0) AS k FROM dim_product").first()["k"]
w2 = Window.orderBy("ProductID")

new_rows = (need_insert
    .withColumn("ProductKey", (F.row_number().over(w2) + F.lit(start)).cast("long"))
    .withColumn("EffectiveFrom", F.col("ModifiedAt"))
    .withColumn("EffectiveTo", F.lit(None).cast("timestamp"))
    .withColumn("IsCurrent", F.lit(True))
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select("ProductKey", "ProductID", "ProductName", "Brand", "Category", "Subcategory", "SupplierID",
            "UnitCost", "EffectiveFrom", "EffectiveTo", "IsCurrent", "_batch_id", "_silver_updated_at"))

print("new rows:", new_rows.count())
new_rows.write.format("delta").mode("append").saveAsTable("dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    MERGE INTO dim_product t USING incoming s
    ON t.ProductID = s.ProductID AND t.IsCurrent = true
    WHEN MATCHED AND s.ModifiedAt > t.EffectiveFrom
         AND NOT (t.UnitCost <=> s.UnitCost AND t.ProductName <=> s.ProductName)
    THEN UPDATE SET t.UnitCost = s.UnitCost, t.ProductName = s.ProductName,
                    t._silver_updated_at = current_timestamp()
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bad = spark.sql("""SELECT COUNT(*) AS n FROM (SELECT ProductID FROM dim_product WHERE IsCurrent = true
                   GROUP BY ProductID HAVING COUNT(*) <> 1)""").first()["n"]
assert bad == 0, "A product does not have exactly one current row"

spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'COMMITTED', RowsRejected = 0,
              LastCommittedWatermark = ActiveHighWatermark, UpdatedAt = current_timestamp()
              WHERE EntityName = '{entity}'""")

spark.sql("SELECT COUNT(*) AS total_rows, SUM(CASE WHEN IsCurrent THEN 1 ELSE 0 END) AS current_rows FROM dim_product").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
