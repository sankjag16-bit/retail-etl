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

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

src_keys = (spark.table("src_keys_order_line")
    .select(F.upper(F.trim("SourceSystem")).alias("SourceSystem"),
            F.trim("OrderID").alias("OrderID"),
            F.col("LineID"))
    .distinct())

silver = spark.table("silver_order_line").filter(~F.col("IsDeleted"))

assert src_keys.count() > 0, "Key list is empty, refusing to mark anything as deleted"

gone = silver.join(src_keys, ["SourceSystem", "OrderID", "LineID"], "left_anti")
print(silver.count(), src_keys.count(), gone.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gone.select("SourceSystem", "OrderID", "LineID", "ModifiedAt").orderBy("OrderID", "LineID").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

limit = int(silver.count() * 0.05)
assert gone.count() <= limit, f"Too many deletes ({gone.count()}), more than 5 percent of Silver. Check the key list."

(DeltaTable.forName(spark, "silver_order_line").alias("t")
    .merge(gone.select("SourceSystem", "OrderID", "LineID").alias("s"),
           "t.SourceSystem = s.SourceSystem AND t.OrderID = s.OrderID AND t.LineID = s.LineID")
    .whenMatchedUpdate(set={"IsDeleted": "true", "_silver_updated_at": "current_timestamp()"})
    .execute())

spark.sql("SELECT IsDeleted, COUNT(*) AS rows FROM silver_order_line GROUP BY IsDeleted").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_keys = spark.table("silver_order_line").select("SourceSystem", "OrderID", "LineID").distinct()
quar_keys = spark.table("quarantine_order_line").select("SourceSystem", "OrderID", "LineID").distinct()

missing = (src_keys
    .join(silver_keys, ["SourceSystem", "OrderID", "LineID"], "left_anti")
    .join(quar_keys, ["SourceSystem", "OrderID", "LineID"], "left_anti"))

print(missing.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
