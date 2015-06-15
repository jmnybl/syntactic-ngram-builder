syntactic-ngram-builder
=======================

An open-source tool to generate syntactic n-grams from a syntactically parsed data. The syntactic n-grams follow the same format as used in the Google Ngram Collection (http://googleresearch.blogspot.fi/2013/05/syntactic-ngrams-over-time.html).

# Input

At the moment the default supported input format is CONLL-U but CONLL-09 is also supported. The extended n-grams are defined for the Stanford Dependencies (SD) and Universal Dependencies (UD) schemes.

# Generating n-grams

    python build_ngrams.py input.conllu --ngrams --args --out_dir output_directory
    
The input can be either a file or a directory containing multiple files. Files must be in CONLL-U format, and end with .gz, .conllu or .conll. Use `--ngrams` to generate syntactic n-grams from nodes to quadarcs (and their extended variants) and `--args` to generate noun-arguments and verb-arguments. You can also have both option at the same time. `--out_dir` is the directory where the resulting n-gram files get created. Alternatively, `--stdout` can be used to print n-grams into standard output, but note that then all n-grams with different length are mixed, and a each line starts with a dataset name followed by the actual n-gram. N-gram builder uses multiprocessing, ad the number of builder processes can be set with `-p` (default is 4). You should however note that also file reader and writers have their own processes.

# Sorting and counting n-grams

    ./sort.sh output_directory
    
The n-grams produced by the software are not unique. Thus, a separate step to sort and count the n-grams is needed. If you have a lot of data to sort and a powerful machine (more than 50G of memory and 20 cores), you can also use the optimized version of the sort script (`./fast_sort.sh out_dir`). For more information about the optimized sort, see https://gist.github.com/fginter/2d4662faeef79acdb772.
