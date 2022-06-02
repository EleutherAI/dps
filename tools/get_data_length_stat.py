import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def define_argparser():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input_path', type=Path, required=True)
    parser.add_argument('--output_path', type=Path, required=True)
    parser.add_argument('--tokenizer', type=str, choices=['character', 'word'], default="word")
    
    return parser.parse_args()

def apply_tokenizer(each_type_data, tokenizer):
    if tokenizer == 'word':
        return each_type_data.apply(lambda x: len(x.split(' ')))
    else:
        return each_type_data.apply(lambda x: x.replace(' ', '')).apply(len)

def data_stats_to_excel(input_path, output_path, tokenizer):
    """
    This takes text data and source of that data, called "data type" (ex. nsmc, kowiki, naver_blog_post, etc.), as an input,
    and makes a length statistics excel file (.xlsx) that includes various types of statistics as an output.
    
    The rows of output file take source of text data (data type) and the columns take types of statistics.
    The types of statistics mean values listed below in specific data type.
    - number of data
    - minimum length
    - maximum length
    - mean of length
    - median of length
    - 25th percentile of length
    - 75th percentile of length
    - standard deviation of length
    
    Args:
        input_path (str): specify the path and filename of dataset to analyse.
        output_path (str): specify the path and filename for data length statistics output.
        tokenizer (str, optional): choose tokenizer options from ('character', 'word'). Defaults to 'word'.
    """
    
    # load data
    data = pd.read_excel(input_path, index_col=0)
    
    # make output
    df = pd.DataFrame(index=data.type.unique(),
                      columns=["nums", "min", "max", "mean", "median", 
                               "percentile_25", "percentile_75", "std"])
    
    for type_name, type_data in data.groupby(['type']):
        type_data['text'] = type_data['text'].apply(str)
        each_type_length = apply_tokenizer(type_data['text'], tokenizer)
        
        nb_data = len(each_type_length)
        min_length = np.min(each_type_length)
        max_length = np.max(each_type_length)
        mean_length = round(np.mean(each_type_length), 2)
        median_length = round(np.median(each_type_length), 2)
        percentile_25_length = round(np.percentile(each_type_length, 25), 2)
        percentile_75_length = round(np.percentile(each_type_length, 75), 2)
        std_of_length = round(np.std(each_type_length), 2)
        
        df.loc[type_name, :] = nb_data, min_length, max_length, mean_length, median_length, \
                                percentile_25_length, percentile_75_length, std_of_length
    
    df.to_excel(output_path)

if __name__ == "__main__":
    config = define_argparser()
    data_stats_to_excel(config.input_path, config.output_path, config.tokenizer)