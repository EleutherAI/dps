import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns

# set to show korean data type in boxplot
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

def data_stats_to_excel(data):
    """
    Args:
        data : train dataset for gpt.

    Returns:
        data length statistics excel file : 
            It includes various length statistics.
            The row takes name of data types and the columns takes type of statistics.
    """
    df = pd.DataFrame(index=data.type.unique(),
                      columns=["nums", "min", "max", "mean", "median", "percentile_1", "percentile_3", "std", "qmarks", "fullstop", "alphabet_first", "numbers"])
    
    for idx, type in enumerate(data.type.unique()):
        each_type = data[data['type'] == type]
        each_type_length = each_type['text'].apply(str).apply(len)
        df.iloc[idx, 0] = len(each_type_length)                           # nums
        df.iloc[idx, 1] = np.min(each_type_length)                        # min
        df.iloc[idx, 2] = np.max(each_type_length)                        # max
        df.iloc[idx, 3] = round(np.mean(each_type_length), 2)             # mean
        df.iloc[idx, 4] = round(np.median(each_type_length), 2)           # median
        df.iloc[idx, 5] = round(np.percentile(each_type_length, 25), 2)   # percentile_1
        df.iloc[idx, 6] = round(np.percentile(each_type_length, 75), 2)   # percentile_3
        df.iloc[idx, 7] = round(np.std(each_type_length), 2)              # std
        df.iloc[idx, 8] = round(np.mean(each_type['text'].apply(str).apply(lambda x: '?' in x)), 4)                        # qmarks
        df.iloc[idx, 9] = round(np.mean(each_type['text'].apply(str).apply(lambda x: '.' in x)), 4)                        # fullstop
        df.iloc[idx, 10] = round(np.mean(each_type['text'].apply(str).apply(lambda x: x[0].encode().isalpha())), 4)        # alphabet_first
        df.iloc[idx, 11] = round(np.mean(each_type['text'].apply(str).apply(lambda x: max([y.isdigit() for y in x]))), 4)  # numbers
    
    return df.to_excel("./data_statistics.xlsx")

def boxplot_of_specific_type(data, data_type = None, save_plot_name = "boxplots"):
    """
    Args:
        data : train dataset for gpt.
        data_type (List, optional): specify which data type included in boxplot. Defaults to None.
        save_name (str, optional): specify the output file name. Defaults to "boxplots".
    """
    assert data_type, "please set the specific data type"
    
    if len(data_type) == 1:
        data_of_specific_type = data[data['type'] == data_type]
    else:
        data_of_specific_type = pd.DataFrame(columns=['text', 'type'])
        for type in data_type:
            clip = data[data['type'] == type]
            data_of_specific_type = pd.concat([data_of_specific_type, clip])
    
    data_of_specific_type.reset_index(drop=True, inplace=True)
    data_of_specific_type['text'] = data_of_specific_type['text'].apply(str).apply(len)
    data_of_specific_type.columns = ['length', 'type']
    
    plt.figure()
    sns.boxplot(x="type", y="length", data=data_of_specific_type)
    plt.gcf().set_size_inches(15, 15)
    plt.savefig(f'./plots/{save_plot_name}.png')
    

def boxplots_of_whole_type(data, data_stats, max_plot_counts = 8):
    """

    Args:
        data : train dataset for gpt.
        data_stats : data statistics excel file which is the output from `data_stats_to_excel` func.
        max_plot_counts (int, optional): specify how many data types want to include in each boxplot image. Defaults to 8.
    """
    sorted_data = data_stats.sort_values(by=['max'], ascending=False)
    iteration = len(data_stats) // max_plot_counts + 1
    for i in range(iteration):
        clip_index = list(sorted_data.index[i*max_plot_counts:(i+1)*max_plot_counts])
        boxplot_of_specific_type(data, data_type = clip_index, save_name = f"boxplots of whole type {i + 1}")
