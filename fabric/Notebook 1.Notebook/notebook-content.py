# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "540f519c-a773-4fdc-a16e-fd6af1061a99",
# META       "default_lakehouse_name": "absence_datasource",
# META       "default_lakehouse_workspace_id": "efa13f00-d56b-4522-bc45-33e7b9de7ac5",
# META       "known_lakehouses": [
# META         {
# META           "id": "540f519c-a773-4fdc-a16e-fd6af1061a99"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Absence Reason Cleanup**


# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Todo
#Build Logic for Country, abs_type_optn, serial_no

spark.read.option("header","TRUE") \
          .csv("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/absence_datasource.Lakehouse/Files/SourceFiles/GP_ABS_REASON.csv") \
          .createOrReplaceTempView("GP_ABS_REASON")

#Getting Top Most Effective Dated row
df_abs_reason= spark.sql(""" SELECT R.COUNTRY, R.ABS_TYPE_OPTN, R.ABSENCE_REASON, R.DESCR FROM GP_ABS_REASON R WHERE R.EFFDT = (SELECT MAX(R1.EFFDT) FROM GP_ABS_REASON R1 WHERE R.COUNTRY = R1.COUNTRY AND R.ABS_TYPE_OPTN=R1.ABS_TYPE_OPTN)""") 
df_abs_reason.write.format("delta").mode("overwrite").saveAsTable(table7)

df_abs_reason.head(5)

              

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Absence Event Cleanup** 

# MARKDOWN ********************

# # **Department Table Cleanup** 

# CELL ********************

#Todo
#Build Logic for SETID, delta logic for LastUpdDttm
#delete Table name

table2 = 'DEPT_TBL'
spark.read.option("header","TRUE") \
          .csv("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/absence_datasource.Lakehouse/Files/SourceFiles/DEPT_TBL.csv") \
          .createOrReplaceTempView("DEPT_TBL")

#Getting Top Most Effective Dated and Active rows
df_dept_tbl= spark.sql(""" SELECT D.SETID, D.DEPTID, D.DESCR, D.LASTUPDDTTM FROM DEPT_TBL D WHERE D.EFFDT = (SELECT MAX(D1.EFFDT) FROM DEPT_TBL D1 WHERE D.SETID=D1.SETID AND D.DEPTID=D1.DEPTID) AND D.EFF_STATUS='A'""") 
df_dept_tbl.write.format("delta").mode("overwrite").saveAsTable(table2)
df_dept_tbl.head(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **XLATITEM Table Cleanup** 


# CELL ********************

#Todo
#Build Logic for serialno, delta logic for LastUpdDttm, CONVERT DATE FROM STRING INTO DATE
#delete Table name

table5 = 'XLATITEM'
spark.read.option("header","TRUE") \
          .csv("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/absence_datasource.Lakehouse/Files/SourceFiles/XLATITEM.csv") \
          .createOrReplaceTempView("XLATITEM")

#Getting Top Most Effective Dated and Active rows
df_xlat_tbl= spark.sql(""" SELECT X.FIELDNAME, X.FIELDVALUE, X.XLATLONGNAME, X.LASTUPDDTTM FROM XLATITEM X WHERE X.EFFDT = (SELECT MAX(X1.EFFDT) FROM XLATITEM X1 WHERE X.FIELDNAME=X1.FIELDNAME AND X.FIELDVALUE=X1.FIELDVALUE) AND X.EFF_STATUS='A'""") 
df_xlat_tbl.write.format("delta").mode("overwrite").saveAsTable(table5)
df_xlat_tbl.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

