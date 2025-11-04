[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data]{[lakehouseId = "b5d8d4b2-4006-4574-b116-221d8171f6cc"]}[Data];
[BindToDefaultDestination = true]
shared PS_DEPT_TBL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_DEPT_TBL", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"DESCRSHORT", "COMPANY", "SETID_LOCATION", "LOCATION", "TAX_LOCATION_CD", "MANAGER_ID", "MANAGER_POSN", "BUDGET_YR_END_DT", "BUDGET_LVL", "GL_EXPENSE", "EEO4_FUNCTION", "CAN_IND_SECTOR", "ACCIDENT_INS", "SI_ACCIDENT_NUM", "HAZARD", "ESTABID", "RISKCD", "GVT_DESCR40", "GVT_SUB_AGENCY", "GVT_PAR_LINE2", "GVT_PAR_LINE3", "GVT_PAR_LINE4", "GVT_PAR_LINE5", "GVT_PAR_DESCR2", "GVT_PAR_DESCR3", "GVT_PAR_DESCR4", "GVT_PAR_DESCR5", "FTE_EDIT_INDC", "DEPT_TENURE_FLG", "TL_DISTRIB_INFO", "USE_BUDGETS", "USE_ENCUMBRANCES", "USE_DISTRIBUTION", "BUDGET_DEPTID", "HP_STATS_DEPT_CD", "HP_STATS_DPCD_NW", "HP_STATS_FACULTY", "MANAGER_NAME", "ACCOUNTING_OWNER", "COUNTRY_GRP", "HP_RTBC_OPTION", "HP_AUTO_RTBC", "CLASS_UNIT_NZL", "ORG_UNIT_AUS", "WORK_SECTOR_AUS", "APS_AGENT_CD_AUS", "IND_COMMITTEE_BEL", "NACE_CD_BEL", "LASTUPDDTTM"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_GP_ABS_EVENT = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_GP_ABS_EVENT", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"ORIG_BEGIN_DT", "ABS_ENTRY_SRC", "PRC_EVT_ACTN_OPTN", "VOIDED_IND", "ABSENCE_REASON", "BEGIN_DAY_HRS", "BEGIN_DAY_HALF_IND", "END_DAY_HRS", "END_DAY_HALF_IND", "ALL_DAYS_IND", "START_TIME", "END_TIME", "START_TIME2", "END_TIME2", "EVT_CONFIG1", "EVT_CONFIG2", "EVT_CONFIG3", "EVT_CONFIG4", "EVT_CONFIG1_DT", "EVT_CONFIG2_DT", "EVT_CONFIG3_DT", "EVT_CONFIG4_DT", "EVT_CONFIG1_DEC", "EVT_CONFIG2_DEC", "EVT_CONFIG3_DEC", "EVT_CONFIG4_DEC", "EVT_CONFIG1_MON", "EVT_CONFIG2_MON", "EVT_CONFIG3_MON", "EVT_CONFIG4_MON", "CURRENCY_CD1", "CURRENCY_CD2", "CURRENCY_CD3", "CURRENCY_CD4", "MANAGER_APPR_IND", "OVRD_ENT_VAL", "OVRD_ADJ_VAL", "CAL_RUN_ID", "PYE_RUN_NUM",  "PROCESS_DT", "ABS_EVT_FCST_VAL", "FCST_DTTM", "DURATION_ABS", "DURATION_DYS", "ACTION_DT_SS", "WF_STATUS", "TRANSACTION_NBR_EA", "FIRST_PROC_DT", "ABS_CAN_REASON"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"EMPLID", "EMPL_RCD", "BGN_DT", "PIN_TAKE_NUM", "END_DT", "DURATION_HOURS", "LAST_UPDT_DT"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"DURATION_HOURS", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PS_GP_ABS_SS_STA = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_GP_ABS_SS_STA", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"OPRID", "EMPLID_CURR_APPR", "HGA_APPR_PRCS"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_GP_PIN = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_GP_PIN", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared PS_JOB = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_JOB", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"PER_ORG", "JOBCODE", "POSITION_NBR", "SUPERVISOR_ID", "APPT_TYPE", "MAIN_APPT_NUM_JPN", "POSITION_OVERRIDE", "POSN_CHANGE_RECORD", "EMPL_STATUS", "ACTION", "ACTION_DT", "ACTION_REASON", "TAX_LOCATION_CD", "JOB_ENTRY_DT", "DEPT_ENTRY_DT", "POSITION_ENTRY_DT", "SHIFT", "REG_TEMP", "FULL_PART_TIME", "COMPANY", "PAYGROUP", "BAS_GROUP_ID", "ELIG_CONFIG1", "ELIG_CONFIG2", "ELIG_CONFIG3", "ELIG_CONFIG4", "ELIG_CONFIG5", "ELIG_CONFIG6", "ELIG_CONFIG7", "ELIG_CONFIG8", "ELIG_CONFIG9", "BEN_STATUS", "BAS_ACTION", "COBRA_ACTION", "EMPL_TYPE", "HOLIDAY_SCHEDULE", "STD_HOURS", "STD_HRS_FREQUENCY", "OFFICER_CD", "EMPL_CLASS", "SAL_ADMIN_PLAN", "GRADE", "GRADE_ENTRY_DT", "STEP", "STEP_ENTRY_DT", "GL_PAY_TYPE", "ACCT_CD", "EARNS_DIST_TYPE", "COMP_FREQUENCY", "COMPRATE", "CHANGE_AMT", "CHANGE_PCT", "ANNUAL_RT", "MONTHLY_RT", "DAILY_RT", "ANNL_BENEF_BASE_RT", "SHIFT_RT", "SHIFT_FACTOR", "CURRENCY_CD", "BUSINESS_UNIT", "SETID_JOBCODE", "SETID_SALARY", "SETID_EMPL_CLASS", "REG_REGION", "DIRECTLY_TIPPED", "FLSA_STATUS", "EEO_CLASS", "FUNCTION_CD", "TARIFF_GER", "TARIFF_AREA_GER", "PERFORM_GROUP_GER", "LABOR_TYPE_GER", "SPK_COMM_ID_GER", "HOURLY_RT_FRA", "ACCDNT_CD_FRA", "VALUE_1_FRA", "VALUE_2_FRA", "VALUE_3_FRA", "VALUE_4_FRA", "VALUE_5_FRA", "CTG_RATE", "PAID_HOURS", "PAID_FTE", "PAID_HRS_FREQUENCY", "UNION_FULL_PART", "UNION_POS", "MATRICULA_NBR", "SOC_SEC_RISK_CODE", "UNION_FEE_AMOUNT", "UNION_FEE_START_DT", "UNION_FEE_END_DT", "EXEMPT_JOB_LBR", "EXEMPT_HOURS_MONTH", "WRKS_CNCL_FUNCTION", "INTERCTR_WRKS_CNCL", "CURRENCY_CD1", "PAY_UNION_FEE", "UNION_CD", "BARG_UNIT", "UNION_SENIORITY_DT", "ENTRY_DATE", "LABOR_AGREEMENT", "EMPL_CTG", "EMPL_CTG_L1", "EMPL_CTG_L2", "SETID_LBR_AGRMNT", "WPP_STOP_FLAG", "LABOR_FACILITY_ID", "LBR_FAC_ENTRY_DT", "LAYOFF_EXEMPT_FLAG", "LAYOFF_EXEMPT_RSN", "GP_PAYGROUP", "GP_DFLT_ELIG_GRP", "GP_ELIG_GRP", "GP_DFLT_CURRTTYP", "CUR_RT_TYPE", "GP_DFLT_EXRTDT", "GP_ASOF_DT_EXG_RT", "ADDS_TO_FTE_ACTUAL", "CLASS_INDC", "ENCUMB_OVERRIDE", "FICA_STATUS_EE", "FTE", "PRORATE_CNT_AMT", "PAY_SYSTEM_FLG", "BORDER_WALKER", "LUMP_SUM_PAY", "CONTRACT_NUM", "JOB_INDICATOR", "WRKS_CNCL_ROLE_CHE", "BENEFIT_SYSTEM", "WORK_DAY_HOURS", "REPORTS_TO", "FORCE_PUBLISH", "JOB_DATA_SRC_CD", "ESTABID", "SUPV_LVL_ID", "SETID_SUPV_LVL", "ABSENCE_SYSTEM_CD", "POI_TYPE", "LAST_HIRE_DT", "TERMINATION_DT", "ASGN_START_DT", "LST_ASGN_START_DT", "ASGN_END_DT", "LDW_OVR", "LAST_DATE_WORKED", "EXPECTED_RETURN_DT", "EXPECTED_END_DATE", "AUTO_END_FLG", "LASTUPDDTTM", "LASTUPDOPRID"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_LOCATION_TBL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_LOCATION_TBL", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"DESCR_AC", "DESCRSHORT", "BUILDING", "FLOOR", "SECTOR", "JURISDICTION", "ATTN_TO", "ADDRESS2", "ADDRESS3", "ADDRESS4", "NUM1", "NUM2", "HOUSE_TYPE", "ADDR_FIELD1", "ADDR_FIELD2", "ADDR_FIELD3", "COUNTY", "GEO_CODE", "IN_CITY_LIMIT", "COUNTRY_CODE", "PHONE", "EXTENSION", "FAX", "SETID_SALARY", "SAL_ADMIN_PLAN", "LANG_CD", "REG_REGION", "HOLIDAY_SCHEDULE", "LOCALITY", "CAN_CMA", "CAN_OEE_AREACD", "GEOLOC_CODE", "OFFICE_TYPE", "NCR_SW_CAN", "TBS_OFFICE_CD_CAN", "SPK_COMM_ID_GER", "TARIFF_AREA_GER", "TARIFF_GER", "INDUST_INSP_ID_GER", "NI_REPORT_SW_UK", "GVT_GEOLOC_CD", "GVT_DESIG_AGENT", "MATRICULA_NBR", "LABEL_FORMAT_ID2", "LABEL_FORMAT_ID3", "USG_LBL_FORMAT_ID", "FON_ER_ID_MEX", "FON_OFFICE_MEX", "LOC_TAX_MEX", "LOC_TAX_SPCL_MEX", "ESTABID", "MESSAGE_TEXT2", "TELE_LOCATION", "LASTUPDDTTM"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_PERSONAL_DATA = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_PERSONAL_DATA", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"COUNTRY_NM_FORMAT", "NAME_INITIALS", "NAME_PREFIX", "NAME_SUFFIX", "NAME_ROYAL_PREFIX", "NAME_ROYAL_SUFFIX", "NAME_TITLE", "MIDDLE_NAME", "SECOND_LAST_NAME", "SECOND_LAST_SRCH", "NAME_AC", "PREF_FIRST_NAME", "PARTNER_LAST_NAME", "PARTNER_ROY_PREFIX", "LAST_NAME_PREF_NLD", "NAME_DISPLAY", "NAME_FORMAL", "COUNTRY", "ADDRESS1", "ADDRESS2", "ADDRESS3", "ADDRESS4", "CITY", "NUM1", "NUM2", "HOUSE_TYPE", "ADDR_FIELD1", "ADDR_FIELD2", "ADDR_FIELD3", "COUNTY", "STATE", "POSTAL", "GEO_CODE", "IN_CITY_LIMIT", "MAR_STATUS_DT", "BIRTHPLACE", "BIRTHCOUNTRY", "BIRTHSTATE", "DT_OF_DEATH", "HIGHEST_EDUC_LVL", "FT_STUDENT", "LANG_CD", "ALTER_EMPLID", "ADDRESS1_AC", "ADDRESS2_AC", "ADDRESS3_AC", "CITY_AC", "COUNTRY_OTHER", "ADDRESS1_OTHER", "ADDRESS2_OTHER", "ADDRESS3_OTHER", "ADDRESS4_OTHER", "CITY_OTHER", "COUNTY_OTHER", "STATE_OTHER", "POSTAL_OTHER", "NUM1_OTHER", "NUM2_OTHER", "HOUSE_TYPE_OTHER", "ADDR_FIELD1_OTHER", "ADDR_FIELD2_OTHER", "ADDR_FIELD3_OTHER", "IN_CITY_LMT_OTHER", "GEO_CODE_OTHER", "COUNTRY_CODE", "PHONE", "EXTENSION", "VA_BENEFIT", "CAMPUS_ID", "DEATH_CERTIF_NBR", "FERPA", "PLACE_OF_DEATH", "US_WORK_ELIGIBILTY", "MILITARY_STATUS", "CITIZEN_PROOF1", "CITIZEN_PROOF2", "MEDICARE_ENTLD_DT", "HONSEKI_JPN", "MILITARY_STAT_ITA", "MILITARY_TYPE_ITA", "MILITARY_RANK_ITA", "MILITARY_END_ITA", "ENTRY_DT_FRA", "MILIT_SITUATN_FRA", "CPAMID", "BILINGUALISM_CODE", "HEALTH_CARE_NBR", "HEALTH_CARE_STATE", "MILIT_SITUATN_ESP", "SOC_SEC_AFF_DT", "MILITARY_STAT_GER", "EXPCTD_MILITARY_DT", "HR_RESPONSIBLE_ID", "SMOKER", "SMOKER_DT", "GVT_CRED_MIL_SVCE", "GVT_MILITARY_COMP", "GVT_MIL_GRADE", "GVT_MIL_RESRVE_CAT", "GVT_MIL_SEP_RET", "GVT_MIL_SVCE_END", "GVT_MIL_SVCE_START", "GVT_MIL_VERIFY", "GVT_PAR_NBR_LAST", "GVT_UNIF_SVC_CTR", "GVT_VET_PREF_APPT", "GVT_VET_PREF_RIF", "GVT_CHANGE_FLAG", "GVT_DRAFT_STATUS", "GVT_YR_ATTAINED", "DISABLED_VET", "DISABLED", "GVT_DISABILITY_CD", "GRADE", "SAL_ADMIN_PLAN", "GVT_CURR_AGCY_EMPL", "GVT_CURR_FED_EMPL", "GVT_HIGH_PAY_PLAN", "GVT_HIGH_GRADE", "GVT_PREV_AGCY_EMPL", "GVT_PREV_FED_EMPL", "GVT_SEP_INCENTIVE", "GVT_SEP_INCENT_DT", "GVT_TENURE", "GVT_PAY_PLAN", "BARG_UNIT", "LASTUPDDTTM", "PREF_LAST_NAME", "PREF_MIDDLE_NAME", "PREF_SEC_LAST_NAME", "PREF_NAME_PREFIX", "PREF_NAME_SUFFIX", "PREF_NAME_TITLE", "NAME_FORMAL_PSF", "MIDDLE_NAME_SRCH", "PARTN_LAST_NM_SRCH", "PREF_FIRST_NM_SRCH", "PREF_LAST_NM_SRCH", "PREF_MIDL_NM_SRCH", "PREF_LAST_SEC_SRCH", "NAME_DISPLAY_SRCH", "NAME_FORMAL_SRCH", "LAST_NAME_SRCH", "FIRST_NAME_SRCH"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_SETID_TBL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_SETID_TBL", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared PSXLATITEM = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PSXLATITEM", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [FIELDNAME] = "STATUS" or [FIELDNAME] = "SEX" or [FIELDNAME] = "MAR_STATUS" or [FIELDNAME] = "HR_WF_ACTION"),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"XLATSHORTNAME", "LASTUPDDTTM", "LASTUPDOPRID", "SYNCID", "EFFDT", "EFF_STATUS"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared PS_COUNTRY_TBL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "69652ffd-fa38-4007-900a-0fea18d8a12d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "PS_COUNTRY_TBL", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
