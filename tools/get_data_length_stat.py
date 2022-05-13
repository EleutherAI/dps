import argparse
import numpy as np
import pandas as pd

def define_argparser():
    p = argparse.ArgumentParser()
    
    p.add_argument('--input_path', required=True)
    p.add_argument('--output_path', required=True)
    
    config = p.parse_args()
    
    return config

def data_stats_to_excel(config):
    """
    Args:
        config : 
        --input_path : the path of train dataset for gpt.
        --output_path : the path of data length statistics output.

    Returns:
        data length statistics excel file : 
            It includes various length statistics.
            The row takes name of data types and the columns takes type of statistics.
    """
    
    # load data file
    data = pd.read_excel(config.input_path, sheet_name='sample_data_cases', index_col=0)
    
    # make output file
    df = pd.DataFrame(index=data.type.unique(),
                      columns=["nums", "min", "max", "mean", "median", "percentile_25", "percentile_75", "std", "qmarks", "fullstop", "alphabet_first", "numbers"])
    
    for type_name, type_data in data.groupby(['type']):
        type_data['text'] = type_data['text'].apply(str)
        each_type_length = type_data['text'].apply(len)
        
        nb_data = len(each_type_length)
        
        min_length = np.min(each_type_length)
        max_length = np.max(each_type_length)
        mean_length = round(np.mean(each_type_length), 2)
        median_length = round(np.median(each_type_length), 2)
        percentile_25_length = round(np.percentile(each_type_length, 25), 2)
        percentile_75_length = round(np.percentile(each_type_length, 75), 2)
        std_of_length = round(np.std(each_type_length), 2)
        
        percentage_of_qmarks = round(np.mean(type_data['text'].apply(lambda x: '?' in x)), 4)
        percentage_of_fullstop = round(np.mean(type_data['text'].apply(lambda x: '.' in x)), 4)
        percentage_of_alphabet_first = round(np.mean(type_data['text'].apply(lambda x: x[0].encode().isalpha())), 4)
        percentage_of_numbers = round(np.mean(type_data['text'].apply(lambda x: max([y.isdigit() for y in x]))), 4)
        
        df.loc[type_name, :] = nb_data, \
            min_length, max_length, mean_length, median_length, percentile_25_length, percentile_75_length, std_of_length, \
            percentage_of_qmarks, percentage_of_fullstop, percentage_of_alphabet_first, percentage_of_numbers
    
    df.to_excel(config.output_path)

if __name__ == "__main__":
    config = define_argparser()
    data_stats_to_excel(config)
