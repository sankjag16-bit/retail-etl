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
batch_id = ""
rows_extracted = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

landed = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id).count()
expected = int(rows_extracted)

if landed != expected:
    msg = f"Bronze has {landed} rows but the copy reported {expected}"
    spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'FAILED', LastError = '{msg}',
                  UpdatedAt = current_timestamp() WHERE EntityName = '{entity}'""")
    raise Exception(msg)

spark.sql(f"""UPDATE ctl_watermark SET LoadStatus = 'BRONZE_COMPLETE', RowsExtracted = {landed},
              LastError = NULL, UpdatedAt = current_timestamp() WHERE EntityName = '{entity}'""")

spark.table("ctl_watermark").filter(F.col("EntityName") == entity).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
