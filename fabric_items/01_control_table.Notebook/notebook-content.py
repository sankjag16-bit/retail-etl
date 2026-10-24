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

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

schema = StructType([
    StructField("EntityName", StringType(), False),
    StructField("LastCommittedWatermark",TimestampType(), True),
    StructField("ActiveBatchID", StringType(), True),
    StructField("ActiveHighWatermark", TimestampType(), True),
    StructField("LoadStatus", StringType(), True),
    StructField("RowsExtracted", LongType(), True),
    StructField("RowsRejected", LongType(), True),
    StructField("LastError", StringType(), True),
    StructField("UpdatedAt", TimestampType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

entities = ["Product", "OrderHeader", "OrderLine", "POSLine",
            "InventorySnapshot", "ShipmentLine", "ReturnLine"]

rows = [(e, datetime(1900, 1, 1), None, None, "NEW", None, None, None, datetime.now())
        for e in entities]

print(rows[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ctl = spark.createDataFrame(rows, schema)
ctl.write.format("delta").mode("overwrite").saveAsTable("ctl_watermark")
spark.table("ctl_watermark").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS bronze_order_line")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
