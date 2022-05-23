#!/usr/bin/env python
# coding: utf-8

from datasets import load_dataset
from tqdm import tqdm

mpww = load_dataset("json", data_files="mpww.jsonl")
mpww = mpww["train"]


def graph_set(triples):
    return {" ".join(triple) for triple in triples}


def get_rows_to_keep(current_graphs):
    return [i for i, graph in current_graphs if not any([graph.issubset(current_graph) for j, current_graph in current_graphs if j != i])]


current_page = mpww[0]["title"]
current_graphs = []
rows_to_keep = []
for i, row in tqdm(enumerate(mpww), total=len(mpww)):
    if row["title"] != current_page:
        rows_to_keep.extend(get_rows_to_keep(current_graphs))
        current_graphs = []
        current_page = row["title"]
    current_graphs.append((i, graph_set(row["triples"])))


mpww_filtered = mpww.select(rows_to_keep)


mpww_filtered.push_to_hub("teven/mpww_filtered")
mpww_filtered.to_json("mpww_filtered.jsonl")
