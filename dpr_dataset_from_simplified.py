import argparse
import json
from functools import partial
from time import time
import re
from datetime import datetime
from datasets import load_dataset


def findWholeWord(w):
    return re.compile(r'\b({0})\b'.format(re.escape(w)), flags=re.IGNORECASE).search


def expand_passages_with_triplets(examples, entities):
    expanded_examples = {"text": [], "title": [], "relation": [], "object": [], "alias": [], "description": []}
    for entity_name, text in zip(examples["title"], examples["text"]):
        try:
            entity = entities[entity_name]
            claims = entity["claims"]
        except KeyError:
            continue
        # variable data tied to the claims in the entities file
        matched_claims = [claim for claim in claims if findWholeWord(claim[1].lower())(text.lower())]
        expanded_examples["relation"].extend([claim[0] for claim in matched_claims])
        expanded_examples["object"].extend([claim[1] for claim in matched_claims])
        # repeating data from the entities file
        expanded_examples["alias"].extend([entity["alias"]] * len(matched_claims))
        expanded_examples["description"].extend([entity["description"]] * len(matched_claims))
        # repeating data from the passages dataset
        expanded_examples["title"].extend([entity_name] * len(matched_claims))
        expanded_examples["text"].extend([text] * len(matched_claims))
    return expanded_examples


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities_file", type=str, default="data/simplified_entities.jsonl")
    parser.add_argument("--passages_file", type=str, default="data/20200501.tsv")
    parser.add_argument("--out_file", type=str, required=True)
    parser.add_argument("--fingerprint", type=str, default=None)
    parser.add_argument("--split_ratio", type=int, default=10)
    args = parser.parse_args()
    dpr_dev_data = []
    dpr_train_data = []
    start = time()
    passage_ds = load_dataset("csv", data_files=args.passages_file, delimiter="\t", index_col=0)["train"]
    passage_time = time()
    print(f"passages dataset loaded in {passage_time - start}s")
    all_entities = {}
    with open(args.entities_file) as f:
        for line in f.readlines():
            entity = json.loads(line[:-1])
            all_entities[entity["title"]] = {"claims": [claim for claim in entity["claims"]],
                                             "description": entity["description"],
                                             "alias": entity["alias"]}
    entities_time = time()
    print(f"entities dataset loaded in {entities_time - passage_time}s")
    fingerprint = args.fingerprint if args.fingerprint is not None else datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    passage_ds = passage_ds.map(partial(expand_passages_with_triplets, entities=all_entities), batched=True,
                                num_proc=32, new_fingerprint=fingerprint, remove_columns=passage_ds.column_names)
    passage_ds.to_json(args.out_file, force_ascii=False)
