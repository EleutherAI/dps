# Adding a Spark processor module

A new Spark processor module can be added to the Spark DataFrame-based 
workflow by:
1. Implementing the processor as a callable Python class
2. Adding configuration for that processor in the configuration file


## Implementation

Each processor module must be implemented as a file inside the [df] directory.

  * The implementation should be a class
  * The constructor of the class will receive as arguments:
      - a configuration dictionary (this will be taken from the configuration
	    file)
	  - the name of the column that contains the document
  * The class should be callable (i.e. have a `__call__` method). That will be
    the entry point:
	  - The entry point recceives a Spark DataFrame
	  - Each Dataframe row contains a document; each row will have at least a
	    column with the document contents
	  - The method must process the DataFrame and return the result, _also
	    as a Spark DataFrame_
  * Once the processor is implemented, it can be added to the configuration
    file as a [processor configuration](#processor-configuration)
  * It is also recommended to add documentation for the module in a
    df processor doc folder


## Processor configuration

Each processor configuration contains:
 * a `class` field: this is the only compulsory field, and its value must
   contain the class to instantiate, as a `module.classname` string (e.g.
   `langfilter.LangFilter`), where `module` is the module name inside the
   [udf] folder.
 * any other content in the dictionary forms the parameter that will be
   sent as argument to the class constructor. In particular, arguments to
   configure the behaviour of the processor should go in a `params` field.


## Example

The following code implements a simple toy example that
 * converts all document text to lowercase
 * removes documents with a size less than a minimum
 
```Python

"""
Example processor
"""


from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from typing import Dict


class Example:

    def __init__(self, config: Dict, doc_column: str):

        self.col = doc_column

        params = config.get("params", {})
        self.min_size = params.get("min_size", 100)


    def __repr__(self) -> str:
        """
        This will be used by loggers
        """
        return f"<Example {self.min_size}>"


    def __call__(self, df: DataFrame) -> DataFrame:
        """
        Process a Spark DataFrame
        """

        # Remove documents shorter than the minimum
        df = df.filter(F.length(self.col) >= self.min_size)
		
        # Convert to lowercase
        df = df.withColumn(self.col, F.lower(self.col))

        return df
```


To use this processor, the steps are:
  * add the code to an `example.py` file in the [df] folder
  * add configuration for this processor to the `process_df` configuration
    section (either to the `phase_1` or `phase_2` section), as
	
	     process_df:
		   phase_1:
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


[df]: ../dps/spark_df/df
