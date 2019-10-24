import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import missingno as msno
import seaborn as sns
sns.set(style="ticks")


class DataAnalyser:

    def __init__(self, data):
        self.separator = "_________________________________________________________\n"
        self.data = data.set_index(['study_id'])
        self.categorical = self.data.select_dtypes(include=['object']).copy()
        self.numeric = self.data.select_dtypes(include=['float64', 'int64']).copy()
        if os.path.exists('../resources/Instrument_report.txt'):
            os.remove("../resources/Instrument_report.txt")
            self.f = open("../resources/Instrument_report.txt", "a+")
        else:
            self.f = open("../resources/Instrument_report.txt", "a+")

    def get_sum_of_isnull(self):
        print(self.data.isna().sum())
        return self.data.isna().sum()

    def get_column_summary(self):
        """
        get summary of all the columns
        """
        self.f.write("                COLUMN SUMMARIES             \n")
        for column in self.data.columns:
            self.f.write(self.separator)
            self.f.write(str(self.data[column].describe())+' '+"\n")

    def get_records_with_missing_values(self):
        self.f.write(self.separator)
        self.f.write("               MISSING VALUES         \n")
        # get only rows with one or more NaN values from the data
        null_data = self.data[self.data.isnull().any(axis=1)]
        self.f.write("Columns with missing values \n" + str(null_data.columns[null_data.isna().any()].tolist())+"\n")
        self.f.write(self.separator)
        for site, row in null_data.groupby(['study_id']):
            self.f.write(str(row.index[0])+' has ' +
                         str(row.isnull().sum(axis=0).sum())+" missing values\n")
        self.f.write("\n")

    # plot missing values
    def missing_visualization(self):
        # bar chart
        msno.bar(self.data)
        plt.savefig('../resources/bar.png', bbox_inches='tight')

        # correlation
        msno.heatmap(self.data)
        plt.savefig('../resources/correlation.png', bbox_inches='tight')

        # heat map
        sns.heatmap(self.data.isnull(), cbar=False)
        plt.savefig('../resources/heat_map.png', bbox_inches='tight')

    def set_pair_plot(self, *argv):
        columns_list = []
        for arg in argv:
            columns_list.append(arg)
        dataset = self.data[columns_list]
        df = dataset.select_dtypes(include=['float64', 'int64']).copy()

        g = sns.PairGrid(df)
        g = g.map(plt.scatter)

        x_labels, y_labels = [], []

        for ax in g.axes[-1, :]:
            x_label = ax.xaxis.get_label_text()
            x_labels.append(x_label)
        for ax in g.axes[:, 0]:
            y_label = ax.yaxis.get_label_text()
            y_labels.append(y_label)

        for i in range(len(x_labels)):
            for j in range(len(y_labels)):
                g.axes[j, i].xaxis.set_label_text(x_labels[i])
                g.axes[j, i].yaxis.set_label_text(y_labels[j])
        plt.savefig('../resources/pair_plot.png', bbox_inches='tight')

        return '../resources/pair_plot.png'

    def get_visualizations(self):
        images = []
        for file in os.listdir("../resources/"):
            if file.endswith(".png"):
                images.append("../resources/"+file)
        return images

    def outliers(self):
        q1 = self.numeric.quantile(0.25)
        q3 = self.numeric.quantile(0.75)
        irq = q3 - q1
        self.f.write(self.separator)
        self.f.write("              OUTLIERS         \n")
        self.f.write(self.separator)
        columns_list = []
        for col in self.numeric.columns:

            c = self.numeric[((self.numeric < (q1 - 1.5 * irq)) | (self.numeric > (q3 + 1.5 * irq)))][col].dropna()
            if c.empty:
                columns_list.append(col)
            else:
                self.f.write(str(self.numeric[((self.numeric < (q1 - 1.5 * irq)) | (self.numeric > (q3 + 1.5 * irq)))][col].dropna())+" \n")
                self.f.write(self.separator)
        for cl in columns_list:
            self.f.write(cl+"\t\t has no outliers \n")
        self.f.write(self.separator)

    def get_report(self):
        self.missing_visualization()
        self.get_records_with_missing_values()
        self.outliers()
        self.get_column_summary()
        self.f.close()
        return self.f.name
