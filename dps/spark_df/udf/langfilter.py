"""
Language filtering object
 * Detect language in text records. Divide text in chunks and detect the
   language of each chunks, retain all languages that were detected in more
   than the required minimum ratio of chunks
 * Filter records according to the text language(s)
"""

import re
from os import environ
import random
from pathlib import Path
import urllib.request
import urllib.parse
from collections import defaultdict
from operator import itemgetter

from filelock import FileLock
import pandas as pd
import fasttext
from fasttext.FastText import _FastText

from typing import Dict, List

from ..utils import logging



# Default local directory where to keep the downloaded model
DEFAULT_LOCALDIR = "/tmp"

DEFAULT_CONFIG = {
    # URL for the model
    "model_url": "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
    # Number of lines per chunk
    "chunk_size": 20,
    # Maximum number of chunks to analyze
    "max_chunks": 10,
    # Minimum score in a chunk to accept a detected language
    "min_lang_score": 0.80,
    # Minimum ratio of chunks with valid languages
    "min_chunk_ratio": 0.60,
    # Minimum chunk ratio for a retained language
    "min_lang_ratio": 0.30
}


def model_filename(model_url: str, model_localdir: str) -> Path:
    """
    Return the local filename of the model
      :param model_url: URL the model can be downloaded from
      :param model_localdir: local directory where the model will be placed
    """
    if model_localdir is None:
        model_localdir = environ.get("TMPDIR", DEFAULT_LOCALDIR)
    url_path = urllib.parse.urlparse(model_url).path
    return Path(model_localdir) / Path(url_path).name


def select_chunks(numlines: int, chunk_size: int, max_chunks: int) -> List[str]:
    """
    Randomly select a number of line chunks
    """
    num_chunks = int(numlines/chunk_size + 1)
    chunklist = range(0, num_chunks)
    if num_chunks > max_chunks:
        chunklist = random.sample(chunklist, max_chunks)
    return chunklist


class LangFilter:
    """
    Detect the text language(s) and, optionally, filter documents not
    in required languages
    """

    def __init__(self, config: Dict):
        """
         :param config: configuration dictionary
        """
        self.log = logging.getLogger(__name__)

        # Load config
        self.config = DEFAULT_CONFIG.copy()
        self.config.update(config)
        self.log.info("config: %s", self.config)

        # Prepare language filter function
        lang = self.config.get("keep_lang")
        if lang:
            lang = set([lang] if isinstance(lang, str) else lang)
            self.langfilter = lambda x: any(c in x for c in lang)
        else:
            self.langfilter = None

        # Load the model
        self.model = self._load_model(self.config["model_url"],
                                      self.config.get("model_localdir"))


    def _load_model(self, model_url: str, model_localdir: str) -> _FastText:
        """
        Load the model
        """
        # Get the model filename
        model_file = model_filename(model_url, model_localdir)
        self.log.info("model filename: %s", model_file)

        # Ensure the model file is locally available
        filename = str(model_file)
        with FileLock(filename + ".lock"):
            if not (model_file.is_file() and model_file.stat().st_size > 0):
                self.log.info("downloading %s into %s", model_url, filename)
                urllib.request.urlretrieve(model_url, filename=filename)

        # Load the model
        #return fasttext.load_model(filename)   # avoid useless warning
        return _FastText(filename)


    def _langdetect(self, text: str) -> str:
        """
        Detect the language(s) in the text, splitting by chunks
        """
        # Split by newlines (since fasttext only evaluates single lines)
        lines = re.split(r"[\n\r]+", text)

        # Select the chunks we will analyze
        chunk_size = self.config["chunk_size"]
        chunklist = select_chunks(len(lines), chunk_size,
                                  self.config["max_chunks"])
        num_chunks = len(chunklist)
        self.log.trace("num_chunks=%d", num_chunks)

        # Detect language for each chunk
        res = defaultdict(int)
        valid_chunks = 0
        for chunk in chunklist:

            # Build the chunk and call the model with it
            pos = chunk*chunk_size
            r = self.model.predict(" ".join(lines[pos:pos+chunk_size]), k=1)
            if not r:
                self.log.warn("chunk_pos=%d, no_lang", pos)
                continue
            self.log.trace("chunk_pos=%d, lang=%s", pos, r)

            # Retain all the languages with score greater than the threshold
            is_valid = False
            for lang, prob in zip(*r):
                if prob > self.config["min_lang_score"]:
                    res[lang[9:]] += 1
                    is_valid = True
            valid_chunks += is_valid
            self.log.trace("retained: lang=%s", dict(res))

        if valid_chunks < num_chunks*self.config["min_chunk_ratio"]:
            self.log.trace("failed: min_chunk_ratio")
            return ""

        # Evaluate the language ratios, keeping only the languages above minimum
        minval = self.config["min_lang_ratio"]*num_chunks
        self.log.trace("minval: %f", minval)
        result = sorted(((k[0], k[1]) for k in res.items() if k[1] >= minval),
                        key=itemgetter(1), reverse=True)

        # Return the languages in sorted order
        self.log.debug("result=%s", result)
        return " ".join(r[0] for r in result)


    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
         - Add a "detectedLang" column with the languages detected in the "text"
           column
         - Optionally, filter out rows whose detected language(s) are not in
           the required list
        """
        if self.log.isEnabledFor(logging.TRACE) and "id" in df.columns:
            self.log.trace("langfilter id:\n%s", df["id"].to_string())

        # Detect language(s) and add the column
        df.loc[:, "detectedLang"] = df["text"].map(self._langdetect)

        # Filter to keep only the desired languages
        if self.langfilter:
            before = len(df)
            df = df[df.detectedLang.apply(self.langfilter)]
            self.log.debug("langfilter=%d", before-len(df))

        return df
