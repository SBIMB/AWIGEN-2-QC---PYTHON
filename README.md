A collection of Python scripts written for the AWI-Gen 2 data QC process.

**NB: Your REDCap API keys must be added to `ApiKeys.py` before you can use most of these scripts.**

`RedcapApiHandler.py` is a helper class that contains functions that can be used to transfer data to/from REDCap.

`Main.py` generates the QC reports for each of the selected sites. The script downloads the site data from the REDCap project using the REDCap API and generates missingness and outliers reports.
- `BranchingLogic.py` runs through the branching logic and generates the missingness report
- `DataAnalyser.py` generates the outliers report for the numerical fields
  - Note that outliers that have been dealt with are stored in the REDCap project `AWIGen 2 Exceptions`
  - `SiteFeedbackHandler.py` can be used to upload the returned outliers report to REDCap

`SetFieldMissing.py` is a helper script that can be used to set all missing values for a specific variable to -999 (i.e. missing)

`SowetoPhase1QC.py` is a script that was written to deal with AWIGen 2 data that was not transferred between a test REDCap project and the production project for Soweto. The script is stored here to provide a record of what was done when transferring the data between REDCap projects.

`BiomarkerHandler.py` can be used to upload biomarker data received from the labs as a .csv to the REDCap project `AWI-Gen 2 Biomarker Results`.

`GenerateCsvForDatabase.py` can be used to generate a csv containing all of the AWIGen 2 data stored in REDCap for upload to a PostgreSQL database.

`create_awigen2_table_all_data.sql` is the SQL script I used to create the table that stores all of the AWIGen 2 data.

`Age_recalculation.py` is used to recalculate phase 1 age based on date of birth provided in phase 2.

`analysis_class_phase2.py` contains all python code functions used to produce data for all calculated variables in phase 2.

`awigen_2_data_database.py` is used to merge all the sites REDCap data to form a single file for upload on SQL.

`continousvar_qc.py` is used identify outliers between phases 1 and 2 continous data.

`logic.py` is used to encode -555 (not applicable) based on the branching logic on the questionaire.

`plots_sites.py` is used to produce the pdf outputs for phase 1 and 2 continous data comparisons.

`site_summaries_phase2.py` is used to produce data distributions for all categorical variables in phase 2.

`main.py` Main file for producing the Excel sheet containing outliers from the continuous variables qc logic.

`encoding.py' Script for re-encoding categorical files.

'CreateStatement*site*.py' details the create statement for the individual site table

'CreateStatementall_site_2.py' details the create statement for the all sites table.

'NamingConversion.py' is used to change the variable names on the all_sites table.

'create_all_sites_psycopg.py' is used to apply the allsites CREATE statement in sql.

'create_database_phase2.py' is used to apply the specific sites CREATE statement in sql.

'insert_data_into_database.py' is used to apply the INSERT statement, uploading the individual site data into its respective table after various data transformation processes.

'insert_into_all_sites.py' is used to apply the INSERT statement, uploading the all_sites table into the all_sites table after various data transformation processes.




