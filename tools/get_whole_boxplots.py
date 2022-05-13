import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns
import numpy as np
import os
from get_some_boxplots import boxplot_of_specific_data_type

# set to show korean data type in boxplot
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

def define_argparser():
    p = argparse.ArgumentParser()
    
    p.add_argument('--input_path', required=True)
    p.add_argument('--max_plot_counts', type=int, default=10)
    
    config = p.parse_args()
    
    return config

def boxplots_of_whole_type(config):
    """
    Args:
        config:
            --input_path : the path of train dataset for gpt.
            --max_plot_counts : specify how many data types want to include in each boxplot image. Defaults to 8.
            
    Returns:
        total boxplot images for data length distribution of whole data types in train dataset.
    """
    
    data = pd.read_excel(config.input_path, sheet_name='sample_data_cases', index_col=0)
    max_plot_counts = config.max_plot_counts
    
    max_length_per_type = {}
    for type_name, type_data in data.groupby(['type']):
        each_type_length = type_data['text'].apply(str).apply(len)
        max_length_per_type[type_name] = np.max(each_type_length)
    max_length_per_type = sorted(max_length_per_type.items(), key=lambda x:x[1], reverse=True)
    max_length_per_type = [t[0] for t in max_length_per_type]
    
    iteration = len(max_length_per_type) // max_plot_counts + 1
    for i in range(iteration):
        clip_index = max_length_per_type[i * max_plot_counts:(i + 1) * max_plot_counts]
        boxplot_of_specific_data_type(data, clip_index, f"tools/plots/boxplots_of_whole_type_split_by_{max_plot_counts}_{i + 1}")

if __name__ == "__main__":
    config = define_argparser()
    boxplots_of_whole_type(config)
