import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns
import os

# set to show korean data type in boxplot
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

def define_argparser():
    p = argparse.ArgumentParser()
    
    p.add_argument('--input_path', required=True)
    p.add_argument('--data_type', type=str, nargs="+", required=True)                # nargs="+" option : get at least one argument.
    p.add_argument('--save_dir', type=str, default="tools/plots/boxplots.png")
    
    config = p.parse_args()
    
    return config

def boxplot_of_specific_data_type(data, data_type, save_dir):
    """
    Args:
        data : train dataset for gpt.
        data_type : specify which data type included in boxplot.
        save_dir : specify the output file name. Defaults to "tools/plots/boxplots.png".
    
    Returns:
        a boxplot image for data length distribution of specific data type.
    """
    
    if len(data_type) == 1:
        data_of_specific_type = data[data['type'] == data_type]
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

if __name__ == "__main__":
    config = define_argparser()
    
    data = pd.read_excel(config.input_path, sheet_name='sample_data_cases', index_col=0)
    data_type = config.data_type
    save_dir = config.save_dir
    
    boxplot_of_specific_data_type(data, data_type, save_dir)
