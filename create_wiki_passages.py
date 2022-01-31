import datasets

wiki_text_dump_date = "20200501"
interval = 100


def split_examples(examples):
    splits = []
    titles = []
    for title, text in zip(examples["title"], examples["text"]):
        splitted_text = text.split()
        rejoined_text = [" ".join(splitted_text[i * interval:(i + 1) * interval]) for i in range(len(splitted_text) // interval)]
        splits.extend(rejoined_text)
        titles.extend([title] * len(rejoined_text))
    return {"title": titles, "text": splits}


if __name__ == "__main__":
    wiki_text = datasets.load_dataset("/home/teven/datasets/datasets/wikipedia", f"{wiki_text_dump_date}.en",
                                      beam_runner='DirectRunner')
    split_wiki = wiki_text.map(split_examples, batched=True)
    split_wiki["train"].to_csv(f"{wiki_text_dump_date}.tsv", sep="\t")
