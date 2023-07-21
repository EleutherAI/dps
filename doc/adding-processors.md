# Adding a processor module

A new processor module can be added to the DataFrame-based workflow by:
1. Implementing the processor as a callable Python class
2. Adding configuration for that processor in the configuration file


## Implementation

Each processor module must be implemented as a file inside the [udf] directory.

  * The implementation should be a class
  * The constructor of the class will receive as argument a configuration
    dictionary (this will be taken from the configuration file). It should
	then initialize any processing objects it needs (e.g. load models)
  * The class should be callable (i.e. have a `__call__` method). That will be
    the entry point:
	  - The entry point recceives a  *Pandas* DataFrame (*not* a Spark DataFrame)
	  - Each Dataframe row contains a document; each row will have at least a
	    `text`column with the document contents
	  - The method must process the DataFrame and return the result, _also
	    as a Pandas DataFrame_
  * Once the processor is implemented, it can be added to the configuration
    file as a [processor configuration](#processor-configuration)
  * It is also recommended to add documentation for the module in the
    [udf processor doc folder](udf)


Note that, since it is called inside a Pandas UDF, a processor _does not work
with Spark types at all_; everything is done in Pandas.

See below for some [examples](#examples)


## Processor configuration

Each processor configuration contains:
 * a `class` field: this is the only compulsory field, and its value must
   contain the class to instantiate, as a `module.classname` string (e.g.
   `langfilter.LangFilter`), where `module` is the module name inside the
   [udf] folder.
 * a `columns` field. This is only required if the processor will create _new_
   columns. If so the field must contain a list of dicts, each dict will have
   two fields:
      - the `name` element contains the name for the new column
	  - the `type` element contains the [pyspark data type] for the column
 * any other content in the dictionary forms the parameter that will be
   sent as argument to the class constructor. In particular, arguments to
   configure the behaviour of the processor should go in a `params` field.


## Examples

The following code implements a simple toy example that
 * converts all document text to lowercase
 * removes documents with a size less than a minimum
 
```Python

"""
Example processor
"""

import pandas as pd

from typing import Dict

from ..utils import logging


def convert(text: str) -> str:
    """
    Convert a text string to lowercase
    Note: we could have done this directly inside the main method,
	here we use a separate function for illustrative purposes
    """
    return text.lower()


class Example:
    """
    Simple example for a processor class
    """

    def __init__(self, config: Dict):
        """
         :param config: configuration dictionary
        """
        self.log = logging.getLogger(__name__)

        # Get the minimum document size from the config (with a default of 100)
        params = config.get("params", {})
        self.min_size = params.get("min_size", 100)


    def __repr__(self) -> str:
        """
        This will be used by loggers
        """
        return f"<Example {self.min_size}>"


    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame
        """
        size = len(df)

        # Keep only the documents larger than the minimum
        df = df[df["text"].str.len() >= self.min_size]

        # Convert the text in each document
        df.loc[:, "text"] = df["text"].map(convert)

        # Log the results, for debugging
        self.log.debug("size removal: %d => %d", size, len(df))

        return df

```

To use this processor, the steps are:
  * add the code to an `example.py` file in the [udf] folder
  * add configuration for this processor to the `process` configuration
    section, as
	
	      - class: example.Example
		    params:
              min_size: 500
  
  
Note that:
  - this processor does not create new columns, so no `columns` field is
    needed in the configuration
  - the text converting code is quite trivial, so we use a simple function
    for it (strictly speaking we did not need even that, we could have used the
	Pandas native method `str.lower()`). If we need to use more elaborate
	processing (esp. to use objects that need heavy initialization), we can
	implement it as a class method instead
	
For examples of those more elaborate procedures, you can check the [splitter]
and the [langfilter] processors.


[udf]: ../dps/spark_df/udf
[splitter]: ../dps/spark_df/udf/splitter.py
[langfilter]: ../dps/spark_df/udf/langfilter.py
[pyspark data type]: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
