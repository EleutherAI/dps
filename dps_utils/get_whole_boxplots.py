# $ python dps_utils/get_whole_boxplots.py 
# --input_path=dps_utils/ko_multi_lang_train_sample.xlsx
# --input_stats_path=dps_utils/data_statistics.xlsx

# --max_plot_counts=6

import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns
import os
from get_some_boxplots import boxplot_of_specific_data_type

# set to show korean data type in boxplot
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

def define_argparser():
    p = argparse.ArgumentParser()
    
    p.add_argument('--input_path', required=True)
    p.add_argument('--input_stats_path', required=True)
    p.add_argument('--max_plot_counts', type=int, default=8)
    
    config = p.parse_args()
    
    return config

def boxplots_of_whole_type(config):
    """
    Args:
        config:
            --input_path : the path of train dataset for gpt.
            --input_stats_path : data statistics excel file which is the output from `get_data_length_stat.py`
            --max_plot_counts : specify how many data types want to include in each boxplot image. Defaults to 8.
            
    Returns:
        total boxplot images for data length distribution of whole data types in train dataset.
    """
    
    DATA_PATH = config.input_path
    DATA_STATS_PATH = config.input_stats_path
    data = pd.read_excel(DATA_PATH, sheet_name='sample_data_cases', index_col=0)
    data_stats = pd.read_excel(DATA_STATS_PATH, index_col=0)
    max_plot_counts = config.max_plot_counts
    
    sorted_data = data_stats.sort_values(by=['max'], ascending=False)
    iteration = len(data_stats) // max_plot_counts + 1
    for i in range(iteration):
        clip_index = list(sorted_data.index[i * max_plot_counts:(i + 1) * max_plot_counts])
        boxplot_of_specific_data_type(data, clip_index, f"dps_utils/plots/boxplots_of_whole_type_{i + 1}")

if __name__ == "__main__":
    config = define_argparser()
    boxplots_of_whole_type(config)
