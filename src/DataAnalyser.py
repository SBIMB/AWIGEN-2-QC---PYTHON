import os
import numpy as np
import pandas as pd
import ImportData
import matplotlib.pyplot as plt
import seaborn as sns
import Instruments


class DataAnalyser:
    instrument = Instruments.Instruments('../resources/data.csv')
    # import_data = ImportData.ImportData()

    def __init__(self):
        self.data = self.instrument.getAnthropometryData().set_index(['study_id'])
        self.categorical = self.data.select_dtypes(include=['object']).copy()
        self.numeric = self.data.select_dtypes(include=['float64', 'int64']).copy()
        if os.path.exists('../resources/report.txt'):
            os.remove("../resources/report.txt")
            self.f = open("../resources/report.txt", "a+")
        else:
            self.f = open("../resources/report.txt", "a+")

    def getSumOfIsnull(self):
        print(self.data.isna().sum())
        return self.data.isna().sum()

    def getColumnSummary(self):
        self.f.write("                COLUMN SUMMARIES             \n")
        for column in self.data.columns:
            self.f.write("_____________________________________________\n")
            self.f.write(str(self.data[column].describe())+' '+"\n")

    def getRecordsWithMissingValues(self):
        self.f.write("________________________________________\n")
        self.f.write("               MISSING VALUES         \n")
        self.f.write("________________________________________\n")
        # get only rows with one or more NaN values from the data
        null_data = self.data[self.data.isnull().any(axis=1)]
        for site, row in null_data.groupby(['gene_uni_site_id_correct']):
            self.f.write(str(site)+':\n'+str(row.index[0])+' has '+
                         str(row.isnull().sum(axis=0).sum())+" missing values\n")
        self.f.write("\n")
        # +str(row['study_id'].to_string(index=False)) +

    def detectUnexpectedIntegerValues(self, col_name):
        # Detecting strings
        cnt = 0
        for row in self.data[col_name]:
            try:
                str(row)
                self.data.loc[cnt, col_name] = np.nan
            except ValueError:
                pass
            cnt += 1

    def detectUnexpectedStringValues(self, col_name):
        # Detecting strings
        cnt = 0
        for row in self.data[col_name]:
            try:
                str(row)
                self.data.loc[cnt, col_name] = np.nan
            except ValueError:
                pass
            cnt += 1

    def checkRangeOfValues(self, col_name):
        # select rows that fall out of range. These can also be nulls
        out_of_range = self.data[self.data[col_name].between(18, 100, inclusive=False) == False]
        print(out_of_range)
        return out_of_range

    # find nulls in branching logic values
    def findNullsInBranchingLogic(self, col_name1, col_name2):
        df = pd.DataFrame()
        try:
            null_entries = self.data.loc[self.data[col_name1].isnull().any(axis=1)]
            df.append(null_entries)
        except ValueError:
            pass
        try:
            first_category = self.data.loc[(self.data[col_name1] == 'A') & (self.data[col_name2].isin(["n/a", "na", "--"]))]
            df.append(first_category)
        except ValueError:
            pass
        try:
            second_category = self.data.loc[(self.data[col_name1] == 'B') & (self.data[col_name2].notna())]
            df.append(second_category)
        except ValueError:
            pass
        print(df)
        return df

    def plotHeatMap(self):
        sns.heatmap(self.data.isnull(), cbar=False)
        plt.savefig('../resources/heat_map.jpeg')
        plt.show()

    def getHeatMap(self):
        return '../resources/heat_map.jpeg'

    def plotBoxPlot(self):
        sns.boxplot(data=self.data, orient="h", palette="Set2",)
        plt.savefig('../resources/box_plot.jpeg')
        plt.show()

    def getBoxPlot(self):
        return '../resources/box_plot.jpeg'

    def outliers(self):
        Q1 = self.numeric.quantile(0.25)
        Q3 = self.numeric.quantile(0.75)
        IQR = Q3 - Q1
        self.f.write("________________________________________\n")
        self.f.write("              OUTLIERS         \n")
        for col in self.numeric.columns:
            self.f.write("_____________________________________________\n")
            self.f.write(str(self.numeric[((self.numeric < (Q1 - 1.5 * IQR)) | (self.numeric > (Q3 + 1.5 * IQR)))][col].dropna())+" \n")

    def getReport(self):
        self.getRecordsWithMissingValues()
        self.outliers()
        self.getColumnSummary()
        self.plotBoxPlot()
        self.plotHeatMap()
        self.f.close()
        return self.f.name


