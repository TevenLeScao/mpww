from time import time
from datasets import load_dataset, Dataset
from tqdm import tqdm

folder = "/home/teven/tetraencoder/datasets/MPWW/"
mpww = load_dataset("json", data_files=f"{folder}/mpww_filtered.jsonl", split="train")
passages = load_dataset("csv", data_files=f"{folder}/passages_with_matches.csv", split="train")

start_time = time()
passages_index = 0
matches = []
for i, item in tqdm(enumerate(mpww), total=len(mpww)):
    while item["text"] != passages[passages_index]["text"]:
        matches.append(None)
        passages_index += 1
    matches.append(i)
    passages_index += 1

for i, match in enumerate(matches):
    if match is not None:
        assert mpww[match]["text"] == passages[i]["text"]

while len(matches) != len(passages):
    matches.append(None)

passages_with_matches = Dataset.from_dict({"title": passages["title"], "text": passages["text"], "mpww_match": matches})
passages_with_matches.to_csv(f"{folder}/passages_with_matches_filtered.csv")
passages_with_matches.push_to_hub("teven/teven/mpww_filtered_all_passages")
