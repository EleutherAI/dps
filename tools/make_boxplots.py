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
    main_parser = argparse.ArgumentParser()
    
    # Common arguments
    main_parser.add_argument("--input_path", type=Path, required=True)
    
    # Generate sub-parser
    program_parser = main_parser.add_subparsers(dest="program")
    
    # Argument only for `make_all_boxplots` program
    parser_A = program_parser.add_parser(name="A")
    parser_A.add_argument("--max_split_cnts", type=int, default=10)
    
    # Argument only for `make_some_boxplots` program
    parser_S = program_parser.add_parser(name="S")
    parser_S.add_argument("--data_type", nargs="+", required=True)   # default type of input argument is string
    
    return main_parser.parse_args()

def make_some_boxplots(data, data_type, save_dir):
    """
    Args:
        data : train dataset for gpt.
        data_type : specify which data type included in boxplot.
        save_dir : specify the output file name. Defaults to "tools/plots/boxplots.png".
    
    Returns:
        a boxplot image for data length distribution of specific data type.
    """
    
    if len(data_type) == 1:
        data_of_specific_type = data[data['type'] == data_type[0]]
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
    output_path = os.path.join(save_dir.split('/')[0], save_dir.split('/')[1])
    os.makedirs(output_path, exist_ok=True)
    plt.savefig(save_dir)
    
def make_all_boxplots(config):
    """
    Args:
        config:
            --input_path : the path of train dataset for gpt.
            --max_split_cnts : specify how many data types want to include in each boxplot image. Defaults to 8.
            
    Returns:
        total boxplot images for data length distribution of whole data types in train dataset.
    """
    
    data = pd.read_excel(config.input_path, sheet_name='sample_data_cases', index_col=0)
    max_split_cnts = config.max_split_cnts
    
    max_length_per_type = {}
    for type_name, type_data in data.groupby(['type']):
        each_type_length = type_data['text'].apply(str).apply(len)
        max_length_per_type[type_name] = np.max(each_type_length)
    max_length_per_type = sorted(max_length_per_type.items(), key=lambda x:x[1], reverse=True)
    max_length_per_type = [t[0] for t in max_length_per_type]
    
    iteration = len(max_length_per_type) // max_split_cnts + 1
    for i in range(iteration):
        clip_index = max_length_per_type[i * max_split_cnts:(i + 1) * max_split_cnts]
        make_some_boxplots(data, clip_index, f"tools/plots/boxplots_of_whole_type_split_by_{max_split_cnts}_{i + 1}")
    
if __name__ == "__main__":
    config = get_args()
    
    if config.program == "A":
        make_all_boxplots(config)
    elif config.program == "S":
        data = pd.read_excel(config.input_path, sheet_name='sample_data_cases', index_col=0)
        data_type = config.data_type
        make_some_boxplots(data, data_type, f"tools/plots/boxplots_for_{' '.join([i for i in data_type])}_data_type.png")
    else:
        raise NotImplementedError(f"No such program {config.program}")