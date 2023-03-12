# Language detection and filtering

This module performs two functions:
 * detects language in the text
 * filters records according to a set of desired languages
 
## I/O

* The constructor expects a configuration dictionary with detection &
  filtering options
* Once instantiated, it is called as a function. It accepts a Pandas DataFrame
  and returns another Pandas DataFrame

Languages are identified by its [ISO 639-1] two-character string.


## Functionality

### Language detection

The module uses the [fasttext] language identification model. It automatically
downloads the model and instantiates it (by default it uses the compressed
lightweight version of the model, but this can be modified with the
`model_url` configuration option)

The process is as follows:
 1. It takes the `text` column of each dataframe row to analyze
 2. It splits the text by newlines
 3. It takes up to 10 random chunks of 20 lines each
 4. For each of these chunks
     - it detects languages in the chunk
	 - it retains languages whose model score is above 0.8
 5. At least 60% of the chunks must contain at least one valid (above 0.8)
    detected language, else the document is considered as having no valid 
	language
 6. It then retains all languages appearing on at least 30% of the chunks,
    reporting them in frequency order (languages most frequenlty used in the
	document chunks go first)
 7. The result is added as a string in a new `detectedLang` column; languages
    are separated by spaces (a row with no valid detected language contains
	an empty string)
	
Those parameters and ratios are the default ones; they can be modified via
configuration options.

### Language filtering

If the configuraion contains a non-empty `keep_lang` field, it should contain
a list of language codes to retain. Then, DataFrame rows whose `detectedLang`
column do not contain _at least one of the languages in the list_ are
discarded from the output.

  
[fasttext]: https://fasttext.cc/docs/en/language-identification.html
[ISO 639-1]: https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
