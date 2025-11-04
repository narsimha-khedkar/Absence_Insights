[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Absence DIM_COUNTRY_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_COUNTRY_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_DEPT_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_DEPT_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_GP_STATUS_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_GP_STATUS_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_JOB_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_JOB_TBL"]}[Data],
  #"Expanded Absence.DIM_DEPT_TBL" = Table.ExpandRecordColumn(#"Navigation 2", "Absence.DIM_DEPT_TBL", {"DESCR"}, {"DESCR"}),
  #"Expanded Absence.DIM_LOCATION_TBL" = Table.ExpandRecordColumn(#"Expanded Absence.DIM_DEPT_TBL", "Absence.DIM_LOCATION_TBL", {"ADDRESS"}, {"ADDRESS"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence.DIM_LOCATION_TBL", {"Absence.DIM_PERSONAL_TBL"})
in
  #"Removed columns 1";
shared #"Absence DIM_LOCATION_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_LOCATION_TBL"]}[Data]
in
  #"Navigation 2";
shared #"Absence DIM_PERSONAL_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_PERSONAL_TBL"]}[Data]
in
  #"Navigation 2";
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
shared #"Absence FACT_ABSENCE_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "FACT_ABSENCE_TBL"]}[Data]
in
  #"Navigation 2";
shared Merge = let
  Source = Table.NestedJoin(#"Absence FACT_ABSENCE_TBL", {"PIN_NUM"}, #"Absence DIM_PIN_TBL", {"PIN_NUM"}, "Absence DIM_PIN_TBL", JoinKind.Inner),
  #"Removed columns" = Table.RemoveColumns(Source, {"Absence.DIM_PERSONAL_TBL", "Absence DIM_PIN_TBL"}),
  #"Expanded Absence.DIM_PIN_TBL" = Table.ExpandRecordColumn(#"Removed columns", "Absence.DIM_PIN_TBL", {"DESCR"}, {"DESCR"})
in
  #"Expanded Absence.DIM_PIN_TBL";
shared #"Merge (2)" = let
  Source = Table.NestedJoin(#"Absence DIM_GP_STATUS_TBL", {"STATUS_ID"}, #"Absence DIM_XLAT_TBL", {"FIELDVALUE_ID"}, "Absence DIM_XLAT_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_XLAT_TBL" = Table.ExpandTableColumn(Source, "Absence DIM_XLAT_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME"})
in
  #"Expanded Absence DIM_XLAT_TBL";
shared #"Abs Complete" = let
  Source = Table.NestedJoin(Merge, {"TRANSACTION_NBR"}, #"Merge (2)", {"TRANSACTION_NBR"}, "Merge (2)", JoinKind.Inner),
  #"Expanded Merge (2)" = Table.ExpandTableColumn(Source, "Merge (2)", {"SUB_DATE", "APR_DATE", "XLATLONGNAME"}, {"SUB_DATE", "APR_DATE", "XLATLONGNAME"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Merge (2)", {{"XLATLONGNAME", "Status"}, {"DESCR", "Abs_Type"}})
in
  #"Renamed columns";
shared Location = let
  Source = Table.NestedJoin(#"Absence DIM_LOCATION_TBL", {"COUNTRY_ID"}, #"Absence DIM_COUNTRY_TBL", {"COUNTRY_ID"}, "Absence DIM_COUNTRY_TBL", JoinKind.Inner),
  #"Removed columns" = Table.RemoveColumns(Source, {"Absence.DIM_JOB_TBL", "Absence.DIM_SETID_TBL", "Absence DIM_COUNTRY_TBL"}),
  #"Expanded Absence.DIM_COUNTRY_TBL" = Table.ExpandRecordColumn(#"Removed columns", "Absence.DIM_COUNTRY_TBL", {"DESCR"}, {"DESCR"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence.DIM_COUNTRY_TBL", {"COUNTRY_ID"})
in
  #"Removed columns 1";
shared #"Merge (4)" = let
  Source = Table.NestedJoin(#"Absence DIM_JOB_TBL", {"LOCATION_ID"}, Location, {"LOCATION_ID"}, "Location", JoinKind.Inner),
  #"Expanded Location" = Table.ExpandTableColumn(Source, "Location", {"ADDRESS", "DESCR"}, {"ADDRESS.1", "DESCR.1"})
in
  #"Expanded Location";
shared #"Job Final" = let
  Source = Table.NestedJoin(#"Merge (4)", {"DEPTID_ID"}, #"Absence DIM_DEPT_TBL", {"DEPTID_ID"}, "Absence DIM_DEPT_TBL", JoinKind.Inner),
  #"Removed columns" = Table.RemoveColumns(Source, {"DEPTID_ID", "LOCATION_ID", "ADDRESS.1"}),
  #"Expanded Absence DIM_DEPT_TBL" = Table.ExpandTableColumn(#"Removed columns", "Absence DIM_DEPT_TBL", {"DESCR"}, {"DESCR.2"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence DIM_DEPT_TBL", {"DESCR.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"DESCR", "Department"}})
in
  #"Renamed columns";
shared #"Merge (5)" = let
  Source = Table.NestedJoin(#"Absence DIM_PERSONAL_TBL", {"SEX"}, #"Absence DIM_XLAT_TBL", {"FIELDVALUE_ID"}, "Absence DIM_XLAT_TBL", JoinKind.Inner),
  #"Removed columns" = Table.RemoveColumns(Source, {"Absence.DIM_JOB_TBL", "Absence.FACT_ABSENCE_TBL"}),
  #"Expanded Absence DIM_XLAT_TBL" = Table.ExpandTableColumn(#"Removed columns", "Absence DIM_XLAT_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded Absence DIM_XLAT_TBL", {"SEX"})
in
  #"Removed columns 1";
shared #"Absence DIM_MAR_STATUS_TBL" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "Absence", Item = "DIM_MAR_STATUS_TBL"]}[Data]
in
  #"Navigation 2";
shared Personal_complete = let
  Source = Table.NestedJoin(#"Merge (5)", {"MAR_STATUS"}, #"Absence DIM_MAR_STATUS_TBL", {"FIELDVALUE_ID"}, "Absence DIM_MAR_STATUS_TBL", JoinKind.Inner),
  #"Expanded Absence DIM_MAR_STATUS_TBL" = Table.ExpandTableColumn(Source, "Absence DIM_MAR_STATUS_TBL", {"XLATLONGNAME"}, {"XLATLONGNAME.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Absence DIM_MAR_STATUS_TBL", {{"XLATLONGNAME", "Gender"}, {"XLATLONGNAME.1", "Marrital Status"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"MAR_STATUS"})
in
  #"Removed columns";
shared #"Merge (6)" = let
  Source = Table.NestedJoin(#"Job Final", {"EMPLID_ID"}, Personal_complete, {"EMPLID_ID"}, "Personal_complete", JoinKind.Inner),
  #"Expanded Personal_complete" = Table.ExpandTableColumn(Source, "Personal_complete", {"NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "Gender", "Marrital Status"}, {"NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "Gender", "Marrital Status"})
in
  #"Expanded Personal_complete";
[BindToDefaultDestination = true]
shared Complete = let
  Source = Table.NestedJoin(#"Abs Complete", {"EMPLID_ID"}, #"Merge (6)", {"EMPLID_ID"}, "Merge (6)", JoinKind.Inner),
  #"Expanded Merge (6)" = Table.ExpandTableColumn(Source, "Merge (6)", {"HIRE_DT", "HOURLY_RT", "Department", "ADDRESS", "DESCR.1", "NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "Gender", "Marrital Status"}, {"HIRE_DT", "HOURLY_RT", "Department", "ADDRESS", "DESCR.1", "NAME", "LAST_NAME", "FIRST_NAME", "BIRTHDATE", "Gender", "Marrital Status"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Merge (6)", {"TRANSACTION_NBR", "EMPLID_ID", "EMPL_RCD", "PIN_NUM"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"DESCR.1", "Country"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"NAME", "FIRST_NAME", "LAST_NAME", "BGN_DT", "END_DT", "Abs_Type", "DURATION_HOURS", "SUB_DATE", "APR_DATE", "Status", "HIRE_DT", "HOURLY_RT", "Department", "ADDRESS", "Country", "BIRTHDATE", "Gender", "Marrital Status"})
in
  #"Reordered columns";
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "efa13f00-d56b-4522-bc45-33e7b9de7ac5"]}[Data]{[warehouseId = "8209ab86-23aa-472c-b77b-ef5771e933a7"]}[Data];
