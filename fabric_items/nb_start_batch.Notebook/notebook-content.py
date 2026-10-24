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

entity = "OrderLine"
bronze_table = "bronze_order_line"
requested_high_wm = "2026-10-05 23:59:59"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import functions as F

fmt = "%Y-%m-%d %H:%M:%S"
row = spark.table("ctl_watermark").filter(F.col("EntityName") == entity).first()

resume = row["LoadStatus"] not in ("NEW", "COMMITTED")
low_wm = row["LastCommittedWatermark"].strftime(fmt)

if resume:
    high_wm = row["ActiveHighWatermark"].strftime(fmt)
    batch_id = row["ActiveBatchID"]
else:
    high_wm = requested_high_wm
    batch_id = entity.lower() + "_" + datetime.strptime(high_wm, fmt).strftime("%Y%m%d%H%M%S")

print(resume, low_wm, high_wm, batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    UPDATE ctl_watermark
    SET ActiveBatchID = '{batch_id}',
        ActiveHighWatermark = CAST('{high_wm}' AS TIMESTAMP),
        LoadStatus = 'STARTED',
        UpdatedAt = current_timestamp()
    WHERE EntityName = '{entity}'
""")

if spark.catalog.tableExists(bronze_table):
    spark.sql(f"DELETE FROM {bronze_table} WHERE _batch_id = '{batch_id}'")

spark.table("ctl_watermark").filter(F.col("EntityName") == entity).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
notebookutils.notebook.exit(json.dumps({"low_wm": low_wm, "high_wm": high_wm, "batch_id": batch_id}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT _batch_id, COUNT(*) AS row_count, MAX(ModifiedAt) AS max_modified
    FROM bronze_order_line GROUP BY _batch_id
""").show(truncate=False)

spark.sql("SELECT LoadStatus, ActiveBatchID FROM ctl_watermark WHERE EntityName = 'OrderLine'").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT _batch_id, COUNT(*) AS row_count, MAX(ModifiedAt) AS max_modified
    FROM bronze_order_line GROUP BY _batch_id
""").show(truncate=False)

spark.sql("SELECT LoadStatus, ActiveBatchID FROM ctl_watermark WHERE EntityName = 'OrderLine'").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
