import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from transformers import AutoTokenizer
import os
import platform
import warnings
warnings.filterwarnings(action='ignore')

# set to show korean data type in boxplot
plt.rcParams['axes.unicode_minus'] = False
if platform.system() == "Darwin":
    plt.rcParams['font.family'] = 'AppleGothic'
elif platform.system() == "Windows":
    assert 'NanumGothic' in [font.name for font in fm.fontManager.ttflist], "please install 'NanumGothic' to show korean name of data type"
    plt.rcParams['font.family'] = 'NanumGothic'
elif platform.system() == "Linux":
    assert 'NanumGothic' in [font.name for font in fm.fontManager.ttflist], "please install 'NanumGothic' to show korean name of data type"
    plt.rcParams["font.family"] = 'NanumGothic'
else:
    raise NotImplementedError(f"{platform.system()} is not supported")

def get_args() -> argparse.Namespace:
    """
    Get specific arguments which user feeds when run `make_boxplots.py`

    - input_path (str): specify the path and filename of dataset to analyse.
    - output_path (str): specify the directory path where you save the output boxplot images. 
                         The filename of boxplot images will be created automatically given arguments.
    - data_type (list, optional): specify which data type will be included in boxplot.
                                  Data type means source of each text data (ex. nsmc, kowiki, naver_blog_post, etc.).
                                  Defaults to all data types in dataset.
    - max_split_cnts (int, optional): specify how many data types will be split and included in each boxplot image. 
                                      Data type means source of each text data (ex. nsmc, kowiki, naver_blog_post, etc.).
                                      Defaults to 10.
    - tokenizer (str, optional): tokenizer options from ('word', 'character', 'huggingface_model_name').
                                'huggingface_model_name' means the name of huggingface model (e.g. 'bert-base-multilingual-cased')
                                 to get the model's tokenizer. Defaults to 'word'.
    """
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--input_path", type=Path, required=True)
    parser.add_argument("--output_path", type=Path, required=True)
    
    parser.add_argument("--data_type", type=str, nargs="*")
    parser.add_argument("--max_split_cnts", type=int, default=10)
    parser.add_argument('--tokenizer', type=str, default="word", 
                        help="You can choose any tokenizer applied in huggingface model (https://huggingface.co/models) by feeding name of huggingface model. \
                        Otherwise, you can simply use word-based or character-based tokenizer by feeding 'word' or 'character'. Defaults to 'word'.")
    
    return parser.parse_args()

def apply_tokenizer(text_data_of_each_type: pd.Series, tokenizer: str) -> pd.Series:
    if tokenizer == 'word':
        return text_data_of_each_type.apply(lambda x: len(x.split(' ')))
    elif tokenizer == 'character':
        return text_data_of_each_type.apply(lambda x: x.replace(' ', '')).apply(len)
    else:
        tokenizer = AutoTokenizer.from_pretrained(tokenizer, bos_token='</s>', eos_token='</s>',
                                                  unk_token='<unk>', pad_token='<pad>', mask_token='<mask>')
        return text_data_of_each_type.apply(lambda x: len(tokenizer.tokenize(x)))

def make_boxplots(data: pd.DataFrame, data_type: list, output_path: Path, max_split_cnts: int = 10, tokenizer: str = 'word') -> None:
    """
    This takes text data and source of that data, called "data type" (ex. nsmc, kowiki, naver_blog_post, etc.), as an input,
    and makes boxplot images to investigate text length distribution by each data type as an output.
    
    The x-axis of boxplot is source of text data (data type) and the y-axis is length split by tokenizer option.

    When user doesn't feed data type (source of text data), whole data types in dataset will be included.
    Otherwise, specific data types will be included.
        
    Args:
        data (str): excel dataset which contains text data and source of that data which is called "data type".
        data_type (list): source of text data that will be included in boxplot along user's input.
        output_path (str): directory path where output boxplot images will be saved.
                           The filename of boxplot images will be created automatically given arguments.
        max_split_cnts (int, optional): how many data types will be split and included in each boxplot image. 
                                        Defaults to 10.
        tokenizer (str, optional): tokenizer options from ('word', 'character', 'huggingface_model_name')
                                   Defaults to 'word'.
    """

    max_length_per_type = {}
    data_of_specific_type = pd.DataFrame(columns=['text', 'type', 'length'])
    
    all_data_types = data['type'].unique()
    for each_type in data_type:
        assert each_type in all_data_types, "please feed data type that is source of text data precisely"
        
        each_type_data = data[data['type'] == each_type]
        each_type_data['length'] = apply_tokenizer(each_type_data['text'], tokenizer)
        
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
        plt.savefig(f"{output_path}/boxplots_about_{number_of_data_type}_number_of_data_type_split_by_{max_split_cnts}_{i + 1}th_tokenized_by_{tokenizer}_level.png")

if __name__ == "__main__":
    config = get_args()

    data = pd.read_excel(config.input_path, index_col=0)
    data['text'] = data['text'].apply(str)
    
    if config.data_type == None:
        make_boxplots(data, list(data['type'].unique()), config.output_path, max_split_cnts=config.max_split_cnts, tokenizer=config.tokenizer)
    else:
        make_boxplots(data, config.data_type, config.output_path, max_split_cnts=config.max_split_cnts, tokenizer=config.tokenizer)