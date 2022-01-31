from time import time
from datasets import load_dataset, Dataset
from create_wiki_passages import wiki_text_dump_date

mpww = load_dataset("teven/mpww")
passages = load_dataset("csv", data_files=f"data/{wiki_text_dump_date}.tsv")

start_time = time()
passages_index = 0
matches = []
for i, item in enumerate(mpww):
    if i % 100000 == 0:
        print(f"{i} out of {len(mpww)} in {time() - start_time}s")
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
