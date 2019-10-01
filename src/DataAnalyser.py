import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="ticks")


class DataAnalyser:

    def __init__(self, data):
        self.data = data.set_index(['study_id'])
        self.categorical = self.data.select_dtypes(include=['object']).copy()
        self.numeric = self.data.select_dtypes(include=['float64', 'int64']).copy()
        if os.path.exists('../resources/report.txt'):
            os.remove("../resources/report.txt")
            self.f = open("../resources/report.txt", "a+")
        else:
            self.f = open("../resources/report.txt", "a+")

    def get_sum_of_isnull(self):
        print(self.data.isna().sum())
        return self.data.isna().sum()

    # get the summaries of every column in the instrument
    def get_column_summary(self):
        self.f.write("                COLUMN SUMMARIES             \n")
        for column in self.data.columns:
            self.f.write("_____________________________________________\n")
            self.f.write(str(self.data[column].describe())+' '+"\n")

    def get_records_with_missing_values(self):
        self.f.write("________________________________________\n")
        self.f.write("               MISSING VALUES         \n")
        # get only rows with one or more NaN values from the data
        null_data = self.data[self.data.isnull().any(axis=1)]
        self.f.write("Columns with missing values \n"+ str(null_data.columns[null_data.isna().any()].tolist())+"\n")
        self.f.write("________________________________________\n")
        for site, row in null_data.groupby(['study_id']):
            self.f.write(str(site)+':\n'+str(row.index[0])+' has '+
                         str(row.isnull().sum(axis=0).sum())+" missing values\n")
        self.f.write("\n")

    def detect_unexpected_integer_values(self, col_name):
        # Detecting strings
        cnt = 0
        for row in self.data[col_name]:
            try:
                str(row)
                self.data.loc[cnt, col_name] = np.nan
            except ValueError:
                pass
            cnt += 1

    def detect_unexpected_string_values(self, col_name):
        # Detecting strings
        cnt = 0
        for row in self.data[col_name]:
            try:
                str(row)
                self.data.loc[cnt, col_name] = np.nan
            except ValueError:
                pass
            cnt += 1

    def check_range_of_values(self, col_name):
        # select rows that fall out of range. These can also be nulls
        out_of_range = self.data[self.data[col_name].between(18, 100, inclusive=False) == False]
        print(out_of_range)
        return out_of_range

    # find nulls in branching logic values
    def find_nulls_in_branching_logic(self, col_name1, col_name2):
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
    # plot graphs
    def plot_heat_map(self):
        sns.heatmap(self.data.isnull(), cbar=False)
        plt.savefig('../resources/heat_map.jpeg')
        plt.show()

    def get_heat_map(self):
        return '../resources/heat_map.jpeg'

    def plot_box_plot(self):
        sns.boxplot(data=self.data, orient="h", palette="Set2",)
        plt.savefig('../resources/box_plot.jpeg')
        plt.show()

    def get_box_plot(self):
        return '../resources/box_plot.jpeg'

    def plot_pairplot(self):
        df1 = self.numeric.iloc[:, 0:int(self.numeric.shape[1] / 2 - 1)]
        df2 = self.numeric.iloc[:, int(self.numeric.shape[1] / 2): int(self.numeric.shape[1])]

        scatter(df1, str(1))
        scatter(df2, str(2))

    def get_pairplot(self):
        a = '../resources/pair_plot1.jpeg'
        b = '../resources/pair_plot2.jpeg'
        return a, b

    def outliers(self):
        q1 = self.numeric.quantile(0.25)
        q3 = self.numeric.quantile(0.75)
        irq = q3 - q1
        self.f.write("________________________________________\n")
        self.f.write("              OUTLIERS         \n")
        for col in self.numeric.columns:
            self.f.write("_____________________________________________\n")
            self.f.write(str(self.numeric[((self.numeric < (q1 - 1.5 * irq)) | (self.numeric > (q3 + 1.5 * irq)))][col].dropna())+" \n")

    def get_report(self):
        self.get_records_with_missing_values()
        self.outliers()
        self.get_column_summary()
        self.plot_box_plot()
        self.plot_heat_map()
        self.plot_pairplot()
        self.f.close()
        return self.f.name


def scatter(df, plt_no):
        g = sns.PairGrid(df)
        g = g.map(plt.scatter)

        xlabels, ylabels = [], []

        for ax in g.axes[-1, :]:
            xlabel = ax.xaxis.get_label_text()
            xlabels.append(xlabel)
        for ax in g.axes[:, 0]:
            ylabel = ax.yaxis.get_label_text()
            ylabels.append(ylabel)

        for i in range(len(xlabels)):
            for j in range(len(ylabels)):
                g.axes[j, i].xaxis.set_label_text(xlabels[i])
                g.axes[j, i].yaxis.set_label_text(ylabels[j])

        plt.savefig('../resources/pair_plot'+plt_no+'.jpeg')
        plt.show()