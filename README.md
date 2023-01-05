# DPS (Data Processing System)

## Requriements

- python 3.8

## How to run DPS?

```bash
python setup.py install
python bin/sparkapp.py {job_name} {params}

# Example
# python bin/sparkapp.py sample_jsonl --config_path=./configs/sample_jsonl.yaml
```

## DPS job list

 job | describe | param options
  -- | -- | --
  `sample_jsonl` | Sample jsonl data from text files in directories | `yaml configs`
  `common_refine_text` | Refine text in common ways | `bash params`

## Development guides

### Test Run

This is test run for `sample_jsonl` job.

#### 1. Setup `dps` package

```bash
python setup.py install
```

#### 2. Check config file and dataset

```bash
cat configs/sample_jsonl.yaml
ls datasets/test_sample_jsonl_data
```

#### 3. Run `sample_jsonl` job by `bin/sparkapp.py`

```bash
python bin/sparkapp.py sample_jsonl --config_path=./configs/sample_jsonl.yaml
```

#### 4. Check output file

```bash
cat datasets/test_output_data/part-00000
```

### Add your own job

#### Implement your job function

0. Make an issue on `ElutherAI/dps` repository
    - Describe your job first
    - Define input and outputs and these examples
1. Go to `dps/spark/jobs` and create python `your_own_job.py` script file.
2. Make a function to run your job. Here's template to play your works.
    ```python
    from pyspark import SparkContext
    from pyspark.rdd import RDD

    from dps.spark.spark_session import spark_session
    from dps.spark.utils.io_utils import read_line, to_json


    def your_own_job(input_path, output_path):
        
        with spark_session(f'your own job') as spark:
            sc: SparkContext = spark.sparkContext # Spark context is to run your spark application

            # Read all files in your directory or file
            proc_rdd: RDD = sc.textFile(input_path) \
                .repartition(10) \
                .flatMap(read_line) 
                
            # Write data that you processed
            proc_rdd \
                .repartition(1) \
                .flatMap(to_json) \
                .saveAsTextFile(output_path)
    ```
3. Register your job into `dps/spark/run.py`
    ```python
    from .jobs.your_own_job import your_own_job

    def run():
        fire.Fire({'sample_jsonl': sample_jsonl,
                   'your_own_job': your_own_job
                   })
    ```

4. Test run your job 
    ```bash
    python bin/sparkapp.py your_own_job --input_path='{input_your_data_dir_or_file}' \
                                        --output_path='{output_path}'
    ```
