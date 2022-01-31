This repo contains code that was used to create the [MPWW](https://huggingface.co/datasets/teven/mpww) dataset. Here are the steps:

1. Download a JSON [dump](https://www.wikidata.org/wiki/Wikidata:Database_download) of Wikidata. The code assumes you're using the latest version.
2. Build the Cython `processing_utils` utility file by running `python setup.py build_ext --inplace`.
3. Index the dataset with `dump_filtering_and_indexing_multiprocessed` (a single-thread version is also provided but it will be slow)
4. Run `simplify_entities.py` to keep only the data that will be used here (this step should probably be merged with 3.)
5. Get the text passages with `create_wiki_passages`; you should probably use a Wikipedia dump with the same date as the Wikidata one.
6. Run `dpr_dataset_from_simplified.py` (a parallel script is provided at `scripts/launch_simplification_in_parallel.sh`)
7. Run `mpww_matching` to create the matched passages dataset.
