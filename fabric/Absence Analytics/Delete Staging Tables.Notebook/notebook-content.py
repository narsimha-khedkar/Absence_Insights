# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b5d8d4b2-4006-4574-b116-221d8171f6cc",
# META       "default_lakehouse_name": "stagingAbsenceData",
# META       "default_lakehouse_workspace_id": "efa13f00-d56b-4522-bc45-33e7b9de7ac5",
# META       "known_lakehouses": [
# META         {
# META           "id": "b5d8d4b2-4006-4574-b116-221d8171f6cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
tables = spark.catalog.listTables()

for table in tables:
    table_name =table.name

    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        print(f" Dropped table: {table_name}")

    except Exception as e:
        print(f" Failed to drop table: {table_name}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
