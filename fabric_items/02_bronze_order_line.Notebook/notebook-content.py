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


entity = "OrderLine"
batch_id = "orderline_run1"
hight_wm = "2026-10-05 23:59:59"

low_wm = spark.table("ctl_watermark")\
                .filter(F.col("EntityName")== entity)\
                .select(F.col("LastCommittedWatermark"))\
                .first()[0]

print(low_wm, hight_wm)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw = spark.read.csv("Files/source/OrderLine.csv", header=True)

window = raw.filter(
    (F.to_timestamp("ModifiedAt") > F.lit(low_wm)) &
    (F.to_timestamp("ModifiedAt") <= F.to_timestamp(F.lit(hight_wm)))
)
window.show(5)
print(window.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze = (window
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("OrderLine.csv"))
)

bronze.write.format("delta").mode("append").saveAsTable("bronze_order_line")

bronze.show(5)

print(spark.table("bronze_order_line").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
