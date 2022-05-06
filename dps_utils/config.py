import argparse
import pandas as pd
from get_data_length_stat import data_stats_to_excel, boxplot_of_specific_type, boxplots_of_whole_type

# load dataset
DATA_PATH = './ko_multi_lang_train_sample.xlsx'
DATA_STAT_PATH = './data_statistics.xlsx'
data = pd.read_excel(DATA_PATH, sheet_name='sample_data_cases', index_col=0)
data_stats = pd.read_excel(DATA_STAT_PATH, index_col=0)

def define_argparser():
    p = argparse.ArgumentParser()
    
    # p.add_argument('--data', required = True)   # "ko_multi_lang_train_sample.xlsx"
    
    p.add_argument('--data_type', type = list, default = None)
    p.add_argument('--max_plot_counts', type = int, default = 8)
    p.add_argument('--save_plot_name', type = str, default = "boxplots")
    
    config = p.parse_args()
    
    return config

def main(config):
    data_stats_to_excel(data)
    boxplot_of_specific_type(data, config.data_type, config.save_plot_name)  # boxplot_of_specific_type(data, data_type=["nsmc", "campuspick_post", "우리말샘"], save_name = "test_plot")
    boxplots_of_whole_type(data, data_stats, config.max_plot_counts)
    

if __name__ == "__main__":
    config = define_argparser()
    main(config)
