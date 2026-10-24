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
entity = "OrderLine"
bronze_table = "bronze_order_line"
rows_extracted = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

landed = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id).count()
expected = int(rows_extracted)
assert landed > 0, f"No Bronze rows for batch {batch_id}"

if landed != expected:
    msg = f"Bronze has {landed} rows but the copy reported {expected}"
    spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'FAILED', LastError = '{msg}',
                  UpdatedAt = current_timestamp() WHERE EntityName = '{entity}'""")
    raise Exception(msg)

spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'BRONZE_COMPLETE', RowsExtracted = {landed},
              LastError = NULL, UpdatedAt = current_timestamp() WHERE EntityName = '{entity}'""")
print(landed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id)
print(bronze.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned = (bronze
    .withColumn("SourceSystem", F.upper(F.trim("SourceSystem")))
    .withColumn("OrderID", F.trim("OrderID"))
    .withColumn("ProductID", F.upper(F.trim("ProductID")))
    .withColumn("ProductID", F.when(F.col("ProductID") == "", F.lit(None)).otherwise(F.col("ProductID"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gross = F.col("Quantity") * F.col("UnitPrice")

reason = F.concat_ws("|",
    F.when(F.col("ProductID").isNull(), F.lit("MISSING_PRODUCT")),
    F.when(F.col("Quantity").isNull(), F.lit("MISSING_QTY")),
    F.when(F.col("Quantity") <= 0, F.lit("BAD_QTY")),
    F.when(F.col("UnitPrice").isNull(), F.lit("MISSING_PRICE")),
    F.when(F.col("UnitPrice") < 0, F.lit("NEGATIVE_PRICE")),
    F.when(F.col("DiscountAmount").isNull(), F.lit("MISSING_DISCOUNT")),
    F.when(F.col("DiscountAmount") < 0, F.lit("NEGATIVE_DISCOUNT")),
    F.when(F.col("DiscountAmount") > gross, F.lit("DISCOUNT_GT_GROSS")))

checked = cleaned.withColumn("_reject_reason", reason)
valid = checked.filter(F.col("_reject_reason") == "")
invalid = checked.filter(F.col("_reject_reason") != "")

total = bronze.count()
v = valid.count()
i = invalid.count()
print(total, v, i, v + i == total)
invalid.groupBy("_reject_reason").count().orderBy(F.desc("count")).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q = "quarantine_order_line"
if spark.catalog.tableExists(q):
    spark.sql(f"DELETE FROM {q} WHERE _batch_id = '{batch_id}'")
invalid.write.format("delta").mode("append").saveAsTable(q)

spark.table(q).groupBy("_batch_id").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

demo = spark.createDataFrame(
    [("W1001", 1, "09:00", 10), ("W1001", 1, "10:00", 15), ("S2001", 1, "10:15", 0)],
    ["OrderID", "LineID", "ModifiedAt", "Discount"])

w = Window.partitionBy("OrderID", "LineID").orderBy(F.col("ModifiedAt").desc())
demo.withColumn("rn", F.row_number().over(w)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = (Window.partitionBy("SourceSystem", "OrderID", "LineID")
           .orderBy(F.col("ModifiedAt").desc(), F.col("_ingested_at").desc()))

ranked = valid.withColumn("_rn", F.row_number().over(w))
latest = ranked.filter(F.col("_rn") == 1).drop("_rn")
extra = ranked.filter(F.col("_rn") > 1)

print(valid.count(), latest.count(), extra.count())

silver_ready = latest.withColumn("LineNetAmount",
                                 F.col("Quantity") * F.col("UnitPrice") - F.col("DiscountAmount"))
silver_ready.select("SourceSystem", "OrderID", "LineID", "Quantity", "UnitPrice",
                    "DiscountAmount", "LineNetAmount").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cols = ["SourceSystem", "OrderID", "LineID", "ProductID", "Quantity", "UnitPrice",
               "DiscountAmount", "ModifiedAt", "LineNetAmount", "_batch_id", "_ingested_at"]

src = (silver_ready.select(*silver_cols)
       .withColumn("IsDeleted", F.lit(False))
       .withColumn("_silver_updated_at", F.current_timestamp()))

if not spark.catalog.tableExists("silver_order_line"):
    src.limit(0).write.format("delta").saveAsTable("silver_order_line")

print(src.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver_order_line")

(target.alias("t")
    .merge(src.alias("s"),
           "t.SourceSystem = s.SourceSystem AND t.OrderID = s.OrderID AND t.LineID = s.LineID")
    .whenMatchedUpdateAll(condition="s.ModifiedAt > t.ModifiedAt")
    .whenNotMatchedInsertAll()
    .execute())

print(spark.table("silver_order_line").count())
(spark.sql("DESCRIBE HISTORY silver_order_line")
      .select("version", "operation", "operationMetrics").show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT SourceSystem, OrderID, LineID, COUNT(*) AS record_count
    FROM silver_order_line
    GROUP BY SourceSystem, OrderID, LineID
    HAVING COUNT(*) > 1
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


bronze_rows = bronze.count()
quarantined = invalid.count()
dupes_dropped = extra.count()
silver_rows = src.count()

print(bronze_rows, quarantined, dupes_dropped, silver_rows)
assert bronze_rows == quarantined + dupes_dropped + silver_rows, "Rows do not reconcile"

dup_keys = spark.sql("""
    SELECT COUNT(*) AS n FROM (
        SELECT 1 FROM silver_order_line
        GROUP BY SourceSystem, OrderID, LineID HAVING COUNT(*) > 1)
""").first()["n"]
assert dup_keys == 0, "Duplicate keys in Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    UPDATE ctl_watermark
    SET LoadStatus = 'SILVER_COMPLETE', RowsRejected = {quarantined}, UpdatedAt = current_timestamp()
    WHERE EntityName = '{entity}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    UPDATE ctl_watermark
    SET LastCommittedWatermark = ActiveHighWatermark, LoadStatus = 'COMMITTED', UpdatedAt = current_timestamp()
    WHERE EntityName = '{entity}'
""")

(spark.table("ctl_watermark").filter(F.col("EntityName") == entity)
      .select("LoadStatus", "RowsExtracted", "RowsRejected", "LastCommittedWatermark", "ActiveHighWatermark")
      .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
