# Document splitting

This processor splits documents into parts, creating a new document for each 
part
 * The "text" column is split into chunks; each chunk generates a new document
 * All other columns in a document are repeated for each new document
 * A new "part" column is added, enumerating each part (starting in 1)
 * The "id" column, if present, is modified to add a suffix with the part, as
   `-N`

## Rules

 * Split points for chunks are found by looking to either:
    - paragraphs, defined as blocks separated by at least 2 newlines
	- full sentences, defined as an end-of-sentence mark (e.g. a period) plus
	  a newline
 * A minimum size for chunks can be defined; if so chunks smaller than the
   minimum are joined with adjacent chunks
 * A maximum size can also be defined; if so chunks larger than the maximum
   are splitted (avoiding splitting in the middle of a word)
   
   
## Configuration

The `params` section in the configuration for this processor can contain the
following options:
 * `use_eos`: indicates if using end-of-sentence is acceptable as splitting
   criterion (this corresponds to the second criterion above)
 * `min_words`: minimum size of a chunk, in words
 * `max_words`: maximum size of a chunk, in words

Words are defined in the standard way, as alphanumeric sequences of characters
