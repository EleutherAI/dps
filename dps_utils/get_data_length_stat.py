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
    DATA_PATH = config.input_path
    data = pd.read_excel(DATA_PATH, sheet_name='sample_data_cases', index_col=0)
    
    # make output file
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
    
    return df.to_excel(config.output_path)

if __name__ == "__main__":
    config = define_argparser()
    data_stats_to_excel(config)
