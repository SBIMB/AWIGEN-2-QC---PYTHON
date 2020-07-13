import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import missingno as msno
import seaborn as sns

import xlsxwriter
import glob
import math

sns.set(style="ticks")

class DataAnalyser:

    def __init__(self, data, instrument_key, resource_dir, excel_writer):
        self.separator = "________________________________________________________________________________\n"
        
        self.data = data.set_index(['study_id'])
        self.categorical = self.data.select_dtypes(include=['object']).copy()
        self.numeric = self.data.select_dtypes(include=[np.number]).copy()

        self.key = instrument_key
        self.excelWriter = excel_writer
        self.resource_dir = resource_dir


        # if os.path.exists(resource_dir + 'Instrument_report.txt'):
        #     os.remove(resource_dir + "Instrument_report.txt")
        #     self.f = open(resource_dir + "Instrument_report.txt", "a+")
        # else:
        #     self.f = open(resource_dir + "Instrument_report.txt", "a+")

    # def write_line(self, line):
    #     self.f.write(line + "\n")

    def get_sum_of_isnull(self):
        print(self.data.isna().sum())
        return self.data.isna().sum()

    # def get_column_summary(self):
    #     """
    #     get summary of all the columns
    #     """
    #     self.f.write("                COLUMN SUMMARIES             \n")
    #     for column in self.data.columns:
    #         self.f.write(self.separator)
    #         self.f.write(str(self.data[column].describe())+' '+"\n")

    # def get_records_with_missing_values(self):
    #     self.f.write(self.separator)
    #     self.f.write("MISSING VALUES - Fields with missing values\n")
    #     self.f.write(self.separator)
    #     self.f.write(self.separator)
    #     self.f.write(" \n")

    #     # null_data2 = self.data.isna()
    #     # nan_count = null_data2.sum()

    #     # Loop through cols and look for NaNs
    #     for col in self.data.columns:
    #         mask = self.data[col].isna()
    #         ids = self.data[mask].index.tolist()
    #         string = (col + "\t" + str(mask.sum()) + " " + " ".join(ids) + "\n").expandtabs(40)
    #         self.f.write(string)
        
    #     self.f.write("\n")

    # plot missing values
    def missing_visualization(self):
        # bar chart
        msno.bar(self.data)
        plt.savefig(self.resource_dir + 'bar.png', bbox_inches='tight')

        # correlation
        msno.heatmap(self.data)
        plt.savefig(self.resource_dir + 'correlation.png', bbox_inches='tight')

        # heat map
        sns.heatmap(self.data.isnull(), cbar=False)
        plt.savefig(self.resource_dir + 'heat_map.png', bbox_inches='tight')

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
        plt.savefig(self.resource_dir + 'pair_plot.png', bbox_inches='tight')

        return self.resource_dir + 'pair_plot.png'

    def get_visualizations(self):
        images = []
        for file in os.listdir(self.resource_dir):
            if file.endswith(".png"):
                images.append(self.resource_dir + file)
        return images

    def plot_histogram(self, data, col, outliers):
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        median = np.round(data.median(),1)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        plt.figure(figsize=(16*0.9, 9*0.9), dpi=200)
        plt.title(col)

        ax = data.plot.hist(bins=100)

        # Plot and label the median
        plt.axvline(median, color='k', linestyle='dashed', linewidth=1)
        plt.text(median*1.0001, ax.dataLim.ymax*0.9995, str('Median: ' + str(median)), fontsize=9)

        # Don't plot the lower boundary if it is below 0 and there are no negative values
        if (lower_bound >= 0) or (data[data < 0].size > 0):
            plt.axvline(lower_bound, color='k', linestyle='dashed', linewidth=1)

        plt.axvline(upper_bound, color='k', linestyle='dashed', linewidth=1)

        # Sort the outliers by value
        outliers = outliers.to_frame().sort_values(by=col)

        # Get the histogram bins and the number of values in each bin
        bar_info = pd.cut(data, 100)
        num_vals = bar_info.value_counts().sort_index()

        # This loop is used to write the outlier IDs above the histogram bins
        id_text_height = 0
        last_bin_idx = -1
        for idx, num in outliers.iterrows():
            current_bin_idx = num_vals.index.get_loc(bar_info[idx])

            # Get the number of values of the bin of the current outlier ID
            num_bin_vals = num_vals.values[current_bin_idx]

            # Reset id_text_height if this is a new bin and update last_bin_idx
            if current_bin_idx != last_bin_idx:
                last_bin_idx = current_bin_idx
                id_text_height = 0

            # If there are more than 30 values in the bin, don't write the IDs
            if num_bin_vals > 30:
                continue

            # Write the ID above the bin, incremented by id_text_height,
            # and increment id_text_height by id_step
            plt.text(bar_info[idx].mid, num_bin_vals + id_text_height, idx, fontsize=7, rotation=40)

            id_step = np.round((ax.dataLim.ymax - num_bin_vals) / 30 , 2)
            id_text_height += id_step

        fig_title = self.resource_dir + col + '_hist.png'

        plt.savefig(fig_title, bbox_inches='tight')
        plt.close()

        return fig_title
        #plt.show()

    def outliers(self):
        # self.f.write(self.separator)
        # self.f.write("FIELD STATS AND OUTLIERS \n")
        # self.f.write("Outliers: Values outside the range (Q1 - 1.5 * IQR, Q3 + 1.5 * IQR) \n")
        # self.f.write(self.separator)
        # self.f.write(self.separator)

        excel_col = 0
        excel_row = 0
        sorted_cols = self.numeric.sort_index(axis=1) #Sort columns by name

        for col in sorted_cols:
            data = self.data[col]

            # Skip iteration if all data is NaN
            if data.dropna().size == 0:
                continue

            q1 = data.quantile(0.25)
            q3 = data.quantile(0.75)
            mean = np.round(data.mean(),1)
            std = np.round(data.std(),1)
            median = np.round(data.median(),1)
            iqr = q3 - q1

            # Skip iteration if IQR = 0
            if iqr == 0:
                # self.f.write('No outliers \n')
                # self.f.write(self.separator)
                continue

            # Find outliers i.e. values outside the range (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
            mask = data.between(q1 - 1.5 * iqr, q3 + 1.5 * iqr, inclusive=True)
            outliers = data[~mask].dropna()

            # Skip iteration if there are no outliers
            if outliers.size == 0:
                continue

            # self.write_line("Field: " + col)
            # self.write_line("Data type: " + data.dtype.name + "\n")

            # self.f.write(str("Mean =\t" + str(mean) + "\n").expandtabs(20))
            # self.f.write(str("Std deviation =\t" + str(std) + "\n").expandtabs(20))
            # self.f.write(str("Median =\t" + str(median) + "\n").expandtabs(20))
            # self.f.write(str("Q1 =\t" + str(q1) + "\n").expandtabs(20))
            # self.f.write(str("Q3 =\t" + str(q3) + "\n").expandtabs(20))
            # self.f.write(str("IQR =\t" + str(np.round(iqr, 1)) + "\n" + "\n").expandtabs(20))

            # Save histogram to .png
            fig_title = self.plot_histogram(data, col, outliers)

            # Write outliers to excel tab starting at excel_row
            outliers.to_excel(self.excelWriter, sheet_name=self.key, startcol=excel_col, startrow=excel_row)
            
            # Get the current excel tab
            self.worksheet = self.excelWriter.sheets[self.key]

            # Make the column with the field name wider
            self.worksheet.set_column(excel_col+1, excel_col+1 , 30)

            #  Insert the histogram .png next to the outliers
            self.worksheet.insert_image(excel_row, excel_col+2, fig_title, {'x_scale': 0.5, 'y_scale': 0.5})

            #Increment the row for the next field
            excel_row += max(18, outliers.size+2)

            # self.write_line(str(outliers.to_string(header=False)))
            # self.f.write(self.separator)

        # self.excelWriter.save()

        # self.f.write(self.separator)


    def generate_report(self):
        #self.missing_visualization()
        #self.get_records_with_missing_values()
        self.outliers()
        #self.get_column_summary()
        #self.f.close()
        #return self.f.name
