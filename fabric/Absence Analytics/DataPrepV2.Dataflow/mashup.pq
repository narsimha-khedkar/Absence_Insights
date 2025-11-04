[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Absence DIM_COUNTRY_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_COUNTRY_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_LOCATION_TBL"})
in
  #"Removed columns";
shared #"Absence DIM_DEPT_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_DEPT_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_JOB_TBL", "Absence.DIM_SETID_TBL"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"DESCR", "DEPARTMENT"}})
in
  #"Renamed columns";
shared #"Absence DIM_GP_STATUS_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_GP_STATUS_TBL"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"STATUS_ID"}, #"Absence DIM_XLAT_TBL", {"FIELDVALUE_ID"}, "Absence DIM_XLAT_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_XLAT_TBL" = Table.ExpandTableColumn(#"Merged queries", "Absence DIM_XLAT_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Absence DIM_XLAT_TBL", {{"XLATLONGNAME", "STATUS"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"STATUS_ID"})
in
  #"Removed columns";
shared #"Absence DIM_JOB_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_JOB_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_DEPT_TBL", "Absence.DIM_LOCATION_TBL", "Absence.DIM_PERSONAL_TBL"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"LOCATION_ID"}, #"Absence DIM_LOCATION_TBL", {"LOCATION_ID"}, "Absence DIM_LOCATION_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_LOCATION_TBL" = Table.ExpandTableColumn(#"Merged queries", "Absence DIM_LOCATION_TBL", {"COUNTRY"}, {"COUNTRY"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded Absence DIM_LOCATION_TBL", {"DEPTID_ID"}, #"Absence DIM_DEPT_TBL", {"DEPTID_ID"}, "Absence DIM_DEPT_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_DEPT_TBL" = Table.ExpandTableColumn(#"Merged queries 1", "Absence DIM_DEPT_TBL", {"DEPARTMENT"}, {"DEPARTMENT"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded Absence DIM_DEPT_TBL", {"EMPLID_ID"}, #"Absence DIM_PERSONAL_TBL", {"EMPLID_ID"}, "Absence DIM_PERSONAL_TBL", JoinKind.LeftOuter),
  #"Expanded Absence DIM_PERSONAL_TBL" = Table.ExpandTableColumn(#"Merged queries 2", "Absence DIM_PERSONAL_TBL", {"NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "GENDER", "MARITAL_STATUS"}, {"NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "GENDER", "MARITAL_STATUS"})
in
  #"Expanded Absence DIM_PERSONAL_TBL";
shared #"Absence DIM_LOCATION_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_LOCATION_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_COUNTRY_TBL", "Absence.DIM_JOB_TBL", "Absence.DIM_SETID_TBL"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"COUNTRY_ID"}, #"Absence DIM_COUNTRY_TBL", {"COUNTRY_ID"}, "Absence DIM_COUNTRY_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_COUNTRY_TBL" = Table.ExpandTableColumn(#"Merged queries", "Absence DIM_COUNTRY_TBL", {"DESCR"}, {"DESCR"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence DIM_COUNTRY_TBL", {"COUNTRY_ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"DESCR", "COUNTRY"}})
in
  #"Renamed columns";
shared #"Absence DIM_MAR_STATUS_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_MAR_STATUS_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_PERSONAL_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_PERSONAL_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_JOB_TBL", "Absence.FACT_ABSENCE_TBL"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"SEX"}, #"Absence DIM_XLAT_TBL", {"FIELDVALUE_ID"}, "Absence DIM_XLAT_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_XLAT_TBL" = Table.ExpandTableColumn(#"Merged queries", "Absence DIM_XLAT_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Absence DIM_XLAT_TBL", {{"XLATLONGNAME", "GENDER"}}),
  #"Merged queries 1" = Table.NestedJoin(#"Renamed columns", {"MAR_STATUS"}, #"Absence DIM_MAR_STATUS_TBL", {"FIELDVALUE_ID"}, "Absence DIM_MAR_STATUS_TBL", JoinKind.FullOuter),
  #"Expanded Absence DIM_MAR_STATUS_TBL" = Table.ExpandTableColumn(#"Merged queries 1", "Absence DIM_MAR_STATUS_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence DIM_MAR_STATUS_TBL", {"SEX", "MAR_STATUS"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"XLATLONGNAME", "MARITAL_STATUS"}})
in
  #"Renamed columns 1";
shared #"Absence DIM_PIN_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_PIN_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_XLAT_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_XLAT_TBL"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Absence FACT_ABSENCE_TBL_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "NAME", DestinationColumnName = "NAME"], [SourceColumnName = "FIRST_NAME", DestinationColumnName = "FIRST_NAME"], [SourceColumnName = "LAST_NAME", DestinationColumnName = "LAST_NAME"], [SourceColumnName = "BGN_DT", DestinationColumnName = "BGN_DT"], [SourceColumnName = "END_DT", DestinationColumnName = "END_DT"], [SourceColumnName = "DURATION_HOURS", DestinationColumnName = "DURATION_HOURS"], [SourceColumnName = "ABS_TYPE", DestinationColumnName = "ABS_TYPE"], [SourceColumnName = "SUB_DATE", DestinationColumnName = "SUB_DATE"], [SourceColumnName = "APR_DATE", DestinationColumnName = "APR_DATE"], [SourceColumnName = "STATUS", DestinationColumnName = "STATUS"], [SourceColumnName = "HIRE_DT", DestinationColumnName = "HIRE_DT"], [SourceColumnName = "HOURLY_RT", DestinationColumnName = "HOURLY_RT"], [SourceColumnName = "COUNTRY", DestinationColumnName = "COUNTRY"], [SourceColumnName = "DEPARTMENT", DestinationColumnName = "DEPARTMENT"], [SourceColumnName = "BIRTHDATE", DestinationColumnName = "BIRTHDATE"], [SourceColumnName = "GENDER", DestinationColumnName = "GENDER"], [SourceColumnName = "MARITAL_STATUS", DestinationColumnName = "MARITAL_STATUS"], [SourceColumnName = "TOTAL_COST", DestinationColumnName = "TOTAL_COST"], [SourceColumnName = "APPROVAL_TIME", DestinationColumnName = "APPROVAL_TIME"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Absence FACT_ABSENCE_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "FACT_ABSENCE_TBL"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Absence.DIM_PERSONAL_TBL", "Absence.DIM_PIN_TBL"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"PIN_NUM"}, #"Absence DIM_PIN_TBL", {"PIN_NUM"}, "Absence DIM_PIN_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_PIN_TBL" = Table.ExpandTableColumn(#"Merged queries", "Absence DIM_PIN_TBL", {"DESCR"}, {"DESCR"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Absence DIM_PIN_TBL", {{"DESCR", "ABS_TYPE"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"PIN_NUM"}),
  #"Merged queries 1" = Table.NestedJoin(#"Removed columns 1", {"TRANSACTION_NBR"}, #"Absence DIM_GP_STATUS_TBL", {"TRANSACTION_NBR"}, "Absence DIM_GP_STATUS_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_GP_STATUS_TBL" = Table.ExpandTableColumn(#"Merged queries 1", "Absence DIM_GP_STATUS_TBL", {"SUB_DATE", "APR_DATE", "STATUS"}, {"SUB_DATE", "APR_DATE", "STATUS"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Absence DIM_GP_STATUS_TBL", {{"EMPL_RCD", Int64.Type}}),
  #"Merged queries 2" = Table.NestedJoin(#"Changed column type", {"EMPLID_ID", "EMPL_RCD"}, #"Absence DIM_JOB_TBL", {"EMPLID_ID", "EMPL_RCD"}, "Absence DIM_JOB_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_JOB_TBL" = Table.ExpandTableColumn(#"Merged queries 2", "Absence DIM_JOB_TBL", {"HIRE_DT", "HOURLY_RT", "COUNTRY", "DEPARTMENT", "NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "GENDER", "MARITAL_STATUS"}, {"HIRE_DT", "HOURLY_RT", "COUNTRY", "DEPARTMENT", "NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "GENDER", "MARITAL_STATUS"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Expanded Absence DIM_JOB_TBL", {"TRANSACTION_NBR", "EMPLID_ID", "EMPL_RCD"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns 2", {"NAME", "FIRST_NAME", "LAST_NAME", "BGN_DT", "END_DT", "DURATION_HOURS", "ABS_TYPE", "SUB_DATE", "APR_DATE", "STATUS", "HIRE_DT", "HOURLY_RT", "COUNTRY", "DEPARTMENT", "BIRTHDATE", "GENDER", "MARITAL_STATUS"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Reordered columns", "TOTAL_COST", each [DURATION_HOURS] * [HOURLY_RT]), {{"TOTAL_COST", type number}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "APPROVAL_TIME", each [APR_DATE] - [SUB_DATE]), {{"APPROVAL_TIME", type number}})
in
  #"Added custom 1";
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data]{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data];
shared #"Absence FACT_ABSENCE_TBL_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  TableNavigation = Navigation_2{[Item = "ABSENCE_PRESENTATION_TBL", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
