"""
Implement a processing pipeline for native Spark DataFrames
"""

from collections import defaultdict

from pyspark.sql import DataFrame

from typing import Dict, List

from .defs import DEFAULT_DOC_COLUMN
from .utils.misc import import_object
from .utils import logging as logging
from .utils.exception import SetupException


BASE_PATH = "dps.spark_df.df."


class DfProcessor:
    """
    The Spark DataFrame processing object. Acts as a Callable
    """

    def __init__(self, config: List[Dict], doc_column: str = None,
                 logconfig: Dict = None):
        """
          :param config: the list of processor configurations
          :param doc_column: name of the column containing the document
          :param logconfig: configuration for the logger
        Store the configuration.
        Delay instantiating objects until call time, so that the DataFrame
        schema is known
        """
        # Check config
        if "phase_1" not in config and "phase_2" not in config:
            raise SetupException("Spark processor config with no phases")
        for phase, preproc_list in config.items():
            for n, preproc in enumerate(preproc_list, start=1):
                if "class" not in preproc:
                    raise SetupException(f"Spark processor config without class definition ({phase}, {n})")

        # Store config
        self.coldoc = doc_column or DEFAULT_DOC_COLUMN
        self.config = config
        self.logconfig = logconfig

        # Look up preprocessors for all phases
        self.proc = defaultdict(list)
        for phase, preproc_list in self.config.items():
            for preproc in preproc_list:
                classname = preproc.pop("class")
                cls = import_object(BASE_PATH + classname)
                self.proc[phase].append((cls, preproc))

        # Delay actual initialization until first use
        self.init = set()


    def _init_obj(self, phase: str):
        """
        Initialize the object
        """
        # Logging
        if self.logconfig:
            logging.basicConfig(**self.logconfig)
            logging.getLogger("py4j").setLevel(logging.ERROR)
            logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

        self.logger = logging.getLogger(__name__)
        self.logger.info("preproc_df %s init", phase)
        self.logger.info(str(self.proc[phase]))

        # Instantiate all processors
        self.proc[phase] = [cls(conf, self.coldoc)
                            for cls, conf in self.proc[phase]]
        self.logger.info("preproc_df %s modules = %s", phase,
                         ', '.join(map(str, self.proc[phase])))

        # Mark as initialized
        self.init.add(phase)


    def __call__(self, phase: str, df: DataFrame) -> DataFrame:
        """
        The processor entry point. Do the processing.
        If necessary, initialize objects
        """
        # Initialize upon first call
        if phase not in self.init:
            self._init_obj(phase)

        self.logger.debug("preproc_df %s", phase)

        # Apply preprocessors
        for proc in self.proc[phase]:
            df = proc(df)

        return df
