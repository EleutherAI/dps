import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns
import os

# set to show korean data type in boxplot
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--input_file_name", type=Path, required=True)
    parser.add_argument("--output_path", type=Path, required=True)
    
    parser.add_argument("--data_type", type=str, nargs="*")
    parser.add_argument("--max_split_cnts", type=int, default=10)
    parser.add_argument('--tokenize', type=str, choices=['character', 'word'], default="word")
    
    return parser.parse_args()

def make_boxplots(data, data_type, output_path, max_split_cnts=10, tokenize='word'):
    """
    Args:
        input_file_name (str): specify the filename of dataset.
        output_path (str): specify the output path.
        
        data_type (list, optional): specify which data type will be included in boxplot. Defaults to None.
        max_split_cnts (int, optional): specify how many data types will be included in each boxplot image. Defaults to 10.
        tokenize (str, optional): choose tokenizing options from ('character', 'word'). Defaults to 'word'.
           
    Returns:
        When user doesn't feed data type, boxplot images for data length distribution of whole data types in dataset will be created.
        Otherwise, a boxplot image for data length distribution of specific data type will be created.
    """

    max_length_per_type = {}
    data_of_specific_type = pd.DataFrame(columns=['text', 'type', 'length'])
    
    for each_type in data_type:
        each_type_data = data[data['type'] == each_type]
        if tokenize == "word":
            each_type_data['length'] = each_type_data['text'].apply(lambda x: len(x.split(' ')))
        else:
            each_type_data['length'] = each_type_data['text'].apply(len)
        
        max_length_per_type[each_type] = max(each_type_data['length'])
        data_of_specific_type = pd.concat([data_of_specific_type, each_type_data])

    data_of_specific_type = data_of_specific_type[['length', 'type']]
    max_length_per_type = sorted(max_length_per_type.items(), key=lambda x:x[1], reverse=True)
    max_length_per_type = [t[0] for t in max_length_per_type]
    number_of_data_type = len(max_length_per_type)
    
    iteration = number_of_data_type // max_split_cnts if number_of_data_type % max_split_cnts == 0 else number_of_data_type // max_split_cnts + 1
    for i in range(iteration):
        clip_index = max_length_per_type[i * max_split_cnts:(i + 1) * max_split_cnts]
        data_for_plot = pd.DataFrame(columns=['length', 'type'])
        
        for dt in clip_index:
            clip = data_of_specific_type[data_of_specific_type['type'] == dt]
            data_for_plot = pd.concat([data_for_plot, clip])

        plt.figure()
        sns.boxplot(x="type", y="length", data=data_for_plot)
        plt.gcf().set_size_inches(20, 20)
        plt.xticks(
            rotation=40,
            horizontalalignment='right',
            fontweight='light',
            fontsize='x-large'
        )
        
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(f"{output_path}/boxplots_about_{number_of_data_type}_number_of_data_type_split_by_{max_split_cnts}_{i + 1}th_tokenized_by_{tokenize}_level.png")

if __name__ == "__main__":
    config = get_args()

    data = pd.read_excel(config.input_file_name, index_col=0)
    data['text'] = data['text'].apply(str)
    
    if config.data_type == None:
        make_boxplots(data, data['type'].unique(), config.output_path, max_split_cnts=config.max_split_cnts, tokenize=config.tokenize)
    else:
        make_boxplots(data, config.data_type, config.output_path, max_split_cnts=config.max_split_cnts, tokenize=config.tokenize)