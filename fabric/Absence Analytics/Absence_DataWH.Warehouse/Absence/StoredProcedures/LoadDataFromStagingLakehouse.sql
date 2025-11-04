CREATE   PROCEDURE Absence.LoadDataFromStagingLakehouse 
 AS
 BEGIN
 	-- Load data into the SETID dimension table
     INSERT INTO Absence.DIM_SETID_TBL (SETID_ID,SETID,DESCR)
     SELECT row_number() over (order by SETID) as SETID_ID, SETID,DESCR
     FROM stagingAbsenceData.dbo.staging_setid_tbl S         
     WHERE NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_SETID_TBL 
         WHERE Absence.DIM_SETID_TBL.SETID = S.SETID         
     );
	 -- Load data into the COUNTRY dimension table
     INSERT INTO Absence.DIM_COUNTRY_TBL (COUNTRY_ID,COUNTRY,DESCR)
     SELECT row_number() over (order by COUNTRY) as COUNTRY_ID, COUNTRY,DESCR
     FROM stagingAbsenceData.dbo.staging_country_tbl C     
     WHERE NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_COUNTRY_TBL 
         WHERE Absence.DIM_COUNTRY_TBL.COUNTRY =C.COUNTRY         
     );
	 -- Load data into the PIN dimension table
     INSERT INTO Absence.DIM_PIN_TBL (PIN_NUM,DESCR)
     SELECT  PIN_NUM,DESCR
     FROM stagingAbsenceData.dbo.staging_pin_tbl P        
     WHERE NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_PIN_TBL 
         WHERE Absence.DIM_PIN_TBL.PIN_NUM = P.PIN_NUM         
     );	 
	 -- Load data into the DEPARTMENT dimension table
     INSERT INTO Absence.DIM_DEPT_TBL (DEPTID_ID,DEPTID,SETID_ID,DESCR)
     SELECT row_number() over (order by A.DEPTID) as DEPTID_ID,A.DEPTID,S.SETID_ID,A.DESCR
     FROM stagingAbsenceData.dbo.staging_dept_tbl  A , Absence.DIM_SETID_TBL S
	 WHERE A.SETID=S.SETID      
     AND NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_DEPT_TBL 
         WHERE Absence.DIM_DEPT_TBL.DEPTID = A.DEPTID         
     );	 
	  -- Load data into the LOCATION dimension table
     INSERT INTO Absence.DIM_LOCATION_TBL (LOCATION_ID,LOCATION,SETID_ID,COUNTRY_ID,ADDRESS)
     SELECT row_number() over (order by A.LOCATION) as LOCATION_ID,A.LOCATION,S.SETID_ID,C.COUNTRY_ID, CONCAT(A.ADDRESS1,' ',A.CITY,' ',A.STATE,' ',A.POSTAL)   AS ADDRESS
     FROM stagingAbsenceData.dbo.staging_location_tbl  A , Absence.DIM_SETID_TBL S, Absence.DIM_COUNTRY_TBL C
	 WHERE A.SETID=S.SETID     AND  C.COUNTRY=A.COUNTRY  
     AND NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_LOCATION_TBL 
         WHERE Absence.DIM_LOCATION_TBL.LOCATION = A.LOCATION         
     );	 
	 -- Load data into the XLAT dimension table
     INSERT INTO Absence.DIM_XLAT_TBL (FIELDVALUE_ID,FIELDVALUE,XLATLONGNAME)
     SELECT row_number() over (order by FIELDVALUE) as FIELDVALUE_ID, FIELDVALUE,XLATLONGNAME
     FROM stagingAbsenceData.dbo.staging_xlat_tbl X        
     WHERE FIELDNAME<>'MAR_STATUS'
	 AND NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_XLAT_TBL 
         WHERE Absence.DIM_XLAT_TBL.FIELDVALUE = X.FIELDVALUE         
     );
	 -- Load data into the Marrital Status dimension table
     INSERT INTO Absence.DIM_MAR_STATUS_TBL (FIELDVALUE_ID,FIELDVALUE,XLATLONGNAME)
     SELECT row_number() over (order by FIELDVALUE) as FIELDVALUE_ID, FIELDVALUE,XLATLONGNAME
     FROM stagingAbsenceData.dbo.staging_xlat_tbl X        
     WHERE FIELDNAME='MAR_STATUS'
	 AND NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_MAR_STATUS_TBL 
         WHERE Absence.DIM_MAR_STATUS_TBL.FIELDVALUE = X.FIELDVALUE     		 
     );
	 -- Load data into the Personal dimension table
     INSERT INTO Absence.DIM_PERSONAL_TBL ([EMPLID_ID],[EMPLID],[NAME],[LAST_NAME],[FIRST_NAME],[SEX],[MAR_STATUS],[BIRTHDATE])
     SELECT row_number() over (order by P.[EMPLID]) as [EMPLID_ID], P.[EMPLID], P.[NAME], P.[LAST_NAME], P.[FIRST_NAME], 
            X1.[FIELDVALUE_ID] AS [SEX], X2.[FIELDVALUE_ID] AS [MAR_STATUS], P.[BIRTHDATE]
     FROM [stagingAbsenceData].[dbo].[staging_personal_tbl] P 
     JOIN [Absence].[DIM_XLAT_TBL] X1 ON X1.[FIELDVALUE] = P.[SEX] -- Auto-Fix: Added table alias P for SEX
     JOIN [Absence].[DIM_MAR_STATUS_TBL] X2 ON X2.[FIELDVALUE] = P.[MAR_STATUS] -- Auto-Fix: Added table alias P for MAR_STATUS
     WHERE NOT EXISTS (
         SELECT 1
         FROM Absence.DIM_PERSONAL_TBL 
         WHERE Absence.DIM_PERSONAL_TBL.[EMPLID] = P.[EMPLID] -- Auto-Fix: Added table alias P for EMPLID
	 );	 
	 -- Load data into the GP Status dimension table
     INSERT INTO Absence.DIM_GP_STATUS_TBL (TRANSACTION_NBR,EMPLID_ID,SUB_DATE,APR_DATE, STATUS_ID)
     SELECT S.TRANSACTION_NBR,P.EMPLID_ID,S.SUB_DATE,S.APR_DATE, X.FIELDVALUE_ID AS STATUS_ID
     FROM stagingAbsenceData.dbo.staging_gp_status_tbl S,  Absence.DIM_PERSONAL_TBL P , Absence.DIM_XLAT_TBL X
     WHERE S.EMPLID=P.EMPLID AND X.FIELDVALUE=S.STATUS
	 AND NOT EXISTS (
     SELECT 1
     FROM Absence.DIM_GP_STATUS_TBL 
     WHERE Absence.DIM_GP_STATUS_TBL.TRANSACTION_NBR = S.TRANSACTION_NBR     		 
     );	   
	-- Load data into the JOB dimension table
    INSERT INTO Absence.DIM_JOB_TBL (EMPLID_ID,EMPL_RCD,DEPTID_ID,LOCATION_ID,HIRE_DT,HOURLY_RT)
     SELECT  P.EMPLID_ID,J.EMPL_RCD,D.DEPTID_ID,L.LOCATION_ID,J.HIRE_DT,J.HOURLY_RT     
     FROM stagingAbsenceData.dbo.staging_job_tbl J,  Absence.DIM_PERSONAL_TBL P ,  Absence.DIM_DEPT_TBL D, Absence.DIM_LOCATION_TBL L, Absence.DIM_SETID_TBL S1, Absence.DIM_SETID_TBL S2
     WHERE J.EMPLID = P.EMPLID AND S1.SETID=J.SETID_DEPT AND D.SETID_ID=S1.SETID_ID AND J.DEPTID = D.DEPTID AND S2.SETID=J.SETID_LOCATION AND L.SETID_ID=S2.SETID_ID AND J.LOCATION = L.LOCATION
	 AND NOT EXISTS (
     SELECT 1
     FROM Absence.DIM_JOB_TBL , Absence.DIM_PERSONAL_TBL 
     WHERE Absence.DIM_JOB_TBL.EMPLID_ID = Absence.DIM_PERSONAL_TBL.EMPLID_ID     	
	 AND Absence.DIM_PERSONAL_TBL.EMPLID = J.EMPLID	 
	 AND Absence.DIM_JOB_TBL.EMPL_RCD = J.EMPL_RCD  
     );	   
	-- Load data into the Absence Event Fact table
     INSERT INTO Absence.FACT_ABSENCE_TBL (TRANSACTION_NBR,EMPLID_ID,EMPL_RCD,PIN_NUM,BGN_DT,END_DT,DURATION_HOURS)
     SELECT S.TRANSACTION_NBR,P.EMPLID_ID,S.EMPL_RCD,S.PIN_TAKE_NUM AS PIN_NUM,S.BGN_DT,S.END_DT,S.DURATION_HOURS
     FROM stagingAbsenceData.dbo.staging_absevt_tbl S,  Absence.DIM_PERSONAL_TBL P 
     WHERE S.EMPLID=P.EMPLID 
	 AND NOT EXISTS (
     SELECT 1
     FROM Absence.FACT_ABSENCE_TBL 
     WHERE Absence.FACT_ABSENCE_TBL.TRANSACTION_NBR = S.TRANSACTION_NBR     		 
	 AND Absence.FACT_ABSENCE_TBL.EMPLID_ID =P.EMPLID_ID     		 
     AND P.EMPLID = S.EMPLID	 
	 AND Absence.FACT_ABSENCE_TBL.BGN_DT = S.BGN_DT     		 
     );	    	
 END