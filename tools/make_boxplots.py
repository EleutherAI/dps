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
    parser.add_argument("--max_split_cnts", type=int, default=10)
    parser.add_argument("--data_type", nargs="*")
    
    return parser.parse_args()

def make_boxplots(input_file_name, output_path, max_split_cnts=10, data_type=None):
    """
    Args:
        input_file_name (str): specify the filename of dataset.
        output_path (str): specify the output path.
        max_split_cnts (int, optional): specify how many data types will be included in each boxplot image. Defaults to 10.
        data_type (list, optional): specify which data type will be included in boxplot. Defaults to None.
           
    Returns:
        When user doesn't feed data type, boxplot images for data length distribution of whole data types in dataset will be created.
        Otherwise, a boxplot image for data length distribution of specific data type will be created.
    """
    
    data = pd.read_excel(input_file_name, index_col=0)
    
    if data_type == None:
        max_length_per_type = {}
        for type_name, type_data in data.groupby(['type']):
            each_type_length = type_data['text'].apply(str).apply(len)
            max_length_per_type[type_name] = np.max(each_type_length)
        max_length_per_type = sorted(max_length_per_type.items(), key=lambda x:x[1], reverse=True)
        max_length_per_type = [t[0] for t in max_length_per_type]
        
        iteration = len(max_length_per_type) // max_split_cnts + 1
        for i in range(iteration):
            clip_index = max_length_per_type[i * max_split_cnts:(i + 1) * max_split_cnts]
            
            data_of_specific_type = pd.DataFrame(columns=['text', 'type'])
            for t in clip_index:
                clip = data[data['type'] == t]
                data_of_specific_type = pd.concat([data_of_specific_type, clip])
            
            data_of_specific_type.reset_index(drop=True, inplace=True)
            data_of_specific_type['text'] = data_of_specific_type['text'].apply(str).apply(len)
            data_of_specific_type.columns = ['length', 'type']
            
            plt.figure()
            sns.boxplot(x="type", y="length", data=data_of_specific_type)
            plt.gcf().set_size_inches(20, 20)
            plt.xticks(
                rotation=40,
                horizontalalignment='right',
                fontweight='light',
                fontsize='x-large'
            )
            
            os.makedirs(output_path, exist_ok=True)
            plt.savefig(f"{output_path}/boxplots_for_whole_data_type_split_by_{max_split_cnts}_{i + 1}.png")
            
    else:
        data_of_specific_type = pd.DataFrame(columns=['text', 'type'])
        for t in data_type:
            clip = data[data['type'] == t]
            data_of_specific_type = pd.concat([data_of_specific_type, clip])
        
        data_of_specific_type.reset_index(drop=True, inplace=True)
        data_of_specific_type['text'] = data_of_specific_type['text'].apply(str).apply(len)
        data_of_specific_type.columns = ['length', 'type']
        
        plt.figure()
        sns.boxplot(x="type", y="length", data=data_of_specific_type)
        plt.gcf().set_size_inches(20, 20)
        plt.xticks(
            rotation=40,
            horizontalalignment='right',
            fontweight='light',
            fontsize='x-large'
        )
        
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(f"{output_path}/boxplots_for_{' '.join([i for i in data_type])}.png")

if __name__ == "__main__":
    config = get_args()

    if config.data_type != None:
        make_boxplots(config.input_file_name, config.output_path, data_type=config.data_type)
    else:
        make_boxplots(config.input_file_name, config.output_path, max_split_cnts=config.max_split_cnts)