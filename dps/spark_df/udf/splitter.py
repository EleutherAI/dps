"""
Splitter preprocessor
 * Take each document and split it in parts, according to options
 * Generate new documents for each part
"""

import re

import pandas as pd

from typing import Dict, List, Iterator

from ..utils import logging
from ..utils.misc import recursive_update, chunker


# End of sentence punctuation for Latin, Devanagari, Chinese & Arabic scripts
EOS = r"[\.\?!।|。！？⋯…؟]+"

# End of paragraph, marked by blank lines
EOP_BLK = r"\n (?:\s*\n){1,}"

# EOP by either:
#  - EOS followed by optional whitespace and at least a newline
#  - an empty line
EOP_EOS = r"(?:" + EOS + r"\s*\n (?:\s*\n)* | \n (?:\s*\n){1,} )"


DEFAULT_CONFIG = {
    # Use end-of-sentence + newline as split point, in addition to 2 newlines
    "use_eos": True,
    # minimum number of words per chunk
    "min_words": 200,
    # maximum number of words per chunk
    "max_words": 1000
}



def add_part(row: pd.Series) -> List[str]:
    """
    Create the part column (packed)
    """
    return list(range(1, len(row.text)+1))

def add_id(row: pd.Series) -> List[str]:
    """
    Update the id column
    """
    return f"{row.id}-{row.part}"



class DocSplitter:
    """
    Split documents into chunks
    """

    def __init__(self, config: Dict):
        """
         :param config: configuration dictionary
        """
        self.log = logging.getLogger(__name__)

        # Load config
        self.config = recursive_update(DEFAULT_CONFIG, config.get("params"))
        self.log.info("config = %s", self.config)

        # Main regular expression
        eos = self.config.get("use_eos", False)
        self.reg = re.compile(r"(" + (EOP_EOS if eos else EOP_BLK) + r")",
                              flags=re.X)

        # Word limits
        self.wmin = int(self.config.get("min_words", 0))
        self.wmax = int(self.config.get("max_words", 0))
        if self.wmin or self.wmax:
            self.ws = re.compile(r"(\W+)")


    def __repr__(self) -> str:
        return f"<DocSplitter {self.wmin},{self.wmax}>"


    def _split_iterator(self, text) -> Iterator[str]:
        """
        Split by paragraphs, possibly with word limits
        """
        # If there are no word limits, just iterate over paragraphs
        if not self.wmin and not self.wmax:
            for para in chunker(self.reg.split(self.doc), 2):
                if para:
                    yield para
            return

        # Iteration with word limits
        prev = ""
        prev_nw = 0
        for para in chunker(self.reg.split(text), 2):

            # Split the paragraph into words (chunks of word+ws), and count them
            words = self.ws.split(para)
            para_nw = len(words) // 2

            # If there is a minimum and we don't reach it, continue iterating
            if self.wmin and prev_nw + para_nw <= self.wmin:
                prev += para
                prev_nw += para_nw
                continue

            # Here we add the chunk to the running buffer, and check

            if not self.wmax or prev_nw + para_nw < self.wmax:

                # If there is no maximum, or we are below it, produce a chunk
                yield prev + para

            else:

                # Here we are above the maximum
                if prev:
                    yield prev  # release the buffer separately

                if para_nw < self.wmax:
                    yield para  # the current chunk is below max
                else:
                    # the chunk is above max, we need to split it
                    words = list(chunker(words, 2))
                    for i in range(0, len(words), self.wmax):
                        yield "".join(words[i:i+self.wmax])

            # Reset
            prev = ""
            prev_nw = 0

        # Last chunk, if present
        if prev:
            yield prev


    def _splitter(self, text: str) -> List[str]:
        """
        Split the text into chunks
        """
        return list(self._split_iterator(text))


    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
         - Split text column into chunks, according to specs
         - Add a "part" column
         - Add the part number to the id
        """
        size_in = len(df)
        has_id = "id" in df.columns
        if self.log.isEnabledFor(logging.TRACE) and has_id:
            self.log.trace("splitter id:\n%s", df["id"].to_string())

        # Split
        df.loc[:, "text"] = df["text"].map(self._splitter)

        # Add part number
        df.loc[:, "part"] = df.apply(add_part, axis=1)

        # Expand the chunks into rows
        df = df.explode(["part", "text"])

        # Update the id column
        if has_id:
            df.loc[:, "id"] = df.apply(add_id, axis=1)

        self.log.debug("split %d => %d", size_in, len(df))
        return df
