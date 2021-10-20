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