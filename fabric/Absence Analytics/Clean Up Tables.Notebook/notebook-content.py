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

# MARKDOWN ********************

# # **Absence Event Cleanup** 

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Department Table Cleanup** 

# CELL ********************

df_dept_tbl=spark.read.parquet("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/stagingAbsenceData.Lakehouse/Tables/PS_DEPT_TBL")         

#Getting Top Most Effective Dated and Active rows
df_dept_tbl= spark.sql(""" SELECT D.SETID, D.DEPTID, D.DESCR FROM PS_DEPT_TBL D WHERE D.EFFDT = (SELECT MAX(D1.EFFDT) FROM PS_DEPT_TBL D1 WHERE D.SETID=D1.SETID AND D.DEPTID=D1.DEPTID) AND D.EFF_STATUS='A'""") 
df_dept_tbl.write.format("delta").mode("overwrite").saveAsTable('STAGING_DEPT_TBL')
#df_location_tbl.head(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Location Table Cleanup** 

# CELL ********************

df_location_tbl=spark.read.parquet("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/stagingAbsenceData.Lakehouse/Tables/PS_LOCATION_TBL")         

#Getting Top Most Effective Dated and Active rows
df_location_tbl= spark.sql(""" SELECT  L.SETID, L.LOCATION, L.DESCR , L.COUNTRY, L.ADDRESS1, L.CITY, L.STATE,L.POSTAL FROM PS_LOCATION_TBL L WHERE L.EFFDT = (SELECT MAX(L1.EFFDT) FROM PS_LOCATION_TBL L1 WHERE L.SETID=L1.SETID AND L.LOCATION=L1.LOCATION) AND L.EFF_STATUS='A'""") 
df_location_tbl.write.format("delta").mode("overwrite").saveAsTable('STAGING_LOCATION_TBL')
#df_location_tbl.head(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **JOB Table Cleanup** 

# CELL ********************

df_job_tbl=spark.read.parquet("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/stagingAbsenceData.Lakehouse/Tables/PS_JOB")         

#Getting Top Most Effective Dated and Active rows
df_job_tbl= spark.sql(""" SELECT  JOBS.EMPLID,JOBS.EMPL_RCD,JOBS.DEPTID,JOBS.LOCATION,JOBS.HOURLY_RT,JOBS.SETID_DEPT,JOBS.SETID_LOCATION,JOBS.HIRE_DT FROM PS_JOB JOBS WHERE JOBS.EFFDT = (SELECT MAX (EFFDT) FROM PS_JOB JOB2 WHERE JOB2.EMPLID = JOBS.EMPLID AND JOB2.EMPL_RCD = JOBS.EMPL_RCD  AND JOB2.EFFDT <=  current_date())AND JOBS.EFFSEQ =(SELECT MAX (L.EFFSEQ)   FROM PS_JOB L  WHERE     L.EMPLID = JOBS.EMPLID AND L.EMPL_RCD = JOBS.EMPL_RCD AND L.EFFDT = JOBS.EFFDT)   AND JOBS.hr_status='A'""") 
df_job_tbl.write.format("delta").mode("overwrite").saveAsTable('STAGING_JOB_TBL')
#df_location_tbl.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **GP STATUS Table Cleanup** 


# CELL ********************

df_gpstatus_tbl=spark.read.parquet("abfss://Absence_Analytics@onelake.dfs.fabric.microsoft.com/stagingAbsenceData.Lakehouse/Tables/PS_GP_ABS_SS_STA")         

#Getting Top Most Effective Dated and Active rows
df_gpstatus_tbl= spark.sql(""" SELECT  DISTINCT TRANSACTION_NBR ,EMPLID , (SELECT MAX(ACTION_DT) FROM  PS_GP_ABS_SS_STA SS WHERE S.TRANSACTION_NBR=SS.TRANSACTION_NBR AND S.EMPLID=SS.EMPLID AND HR_WF_ACTION='SUB') AS SUB_DATE, (SELECT MAX(ACTION_DT) FROM  PS_GP_ABS_SS_STA SS WHERE S.TRANSACTION_NBR=SS.TRANSACTION_NBR AND S.EMPLID=SS.EMPLID AND HR_WF_ACTION='APV') AS APR_DATE,  (SELECT  FIRST(HR_WF_ACTION) FROM  PS_GP_ABS_SS_STA SS  WHERE S.TRANSACTION_NBR=SS.TRANSACTION_NBR AND S.EMPLID=SS.EMPLID and SS.SEQNUM =  (SELECT MAX(SSS.SEQNUM) FROM PS_GP_ABS_SS_STA SSS  WHERE SS.TRANSACTION_NBR=SSS.TRANSACTION_NBR AND SS.EMPLID=SSS.EMPLID )) AS STATUS  FROM PS_GP_ABS_SS_STA S
 """) 
df_gpstatus_tbl.write.format("delta").mode("overwrite").saveAsTable('STAGING_GP_STATUS_TBL')
#df_location_tbl.head(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Rename Tables** 

# CELL ********************

spark.sql("""  ALTER TABLE PS_PERSONAL_DATA RENAME TO STAGING_PERSONAL_TBL""") 
spark.sql("""  ALTER TABLE PS_COUNTRY_TBL RENAME TO STAGING_COUNTRY_TBL""") 
spark.sql(""" ALTER TABLE PS_GP_PIN RENAME TO STAGING_PIN_TBL""") 
spark.sql(""" ALTER TABLE PSXLATITEM RENAME TO STAGING_XLAT_TBL""") 
spark.sql(""" ALTER TABLE PS_SETID_TBL RENAME TO STAGING_SETID_TBL""") 
spark.sql(""" ALTER TABLE PS_GP_ABS_EVENT RENAME TO STAGING_ABSEVT_TBL""") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Delete Tables** 

# CELL ********************

spark.sql("""  DROP TABLE  PS_LOCATION_TBL""") 
spark.sql("""  DROP TABLE  PS_DEPT_TBL""") 
spark.sql("""  DROP TABLE  PS_JOB""") 
spark.sql("""  DROP TABLE  PS_GP_ABS_SS_STA""") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

