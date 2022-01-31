import json
import time

def process():
    cdef double start = time.time()
    cdef int filtered_articles = 0
    cdef dict index = {}
    cdef set accepted_datatypes = {'quantity', 'time', 'wikibase-item'}

    with open("data/latest-all.json", "r") as f, open("data/sample.jsonl", 'w') as g:
        for i, line in enumerate(f):
            if i == 0:
                continue
            if i % 10000 == 0:
                print(i, filtered_articles, "{:.4f}".format(time.time() - start))
            entity = json.loads(line[:-2])
            try:
                if (not 'enwiki' in entity["sitelinks"]) or (not 'en' in entity['labels']) or (
                        not 'en' in entity['descriptions']):
                    continue
                entity['labels'] = {'en': entity['labels']['en']}
                entity['sitelinks'] = {'enwiki': entity['sitelinks']['enwiki']}
                entity['descriptions'] = entity['descriptions']['en']
                index[entity['id']] = {'label': entity['labels']['en'], 'title': entity['sitelinks']['enwiki']}
                entity['claims'] = {
                    prop: [value for value in values if value['mainsnak']['datatype'] in accepted_datatypes] for
                    prop, values in entity['claims'].items()}
                filtered_articles += 1
                g.write(json.dumps(entity, ensure_ascii=False) + "\n")
            except BaseException as error:
                print(error)
    return index

def line_process(line):
    cdef set accepted_datatypes = {'quantity', 'time', 'wikibase-item'}
    cdef dict entity
    cdef str title
    try:
        entity = json.loads(line[:-2])
        title = entity['sitelinks']['enwiki']['title']
        # Normally I'd wrap this up in a function but cython code is faster without it
        if (not 'enwiki' in entity["sitelinks"]) or (not 'en' in entity['labels']) or (
                not 'en' in entity['descriptions']) or "disambiguation" in title or title.startswith(
            "Template:") or title.startswith("Category:") or title.startswith("List of"):
            return None, None, None
        entity['labels'] = {'en': entity['labels']['en']}
        entity['sitelinks'] = {'enwiki': entity['sitelinks']['enwiki']}
        entity['descriptions'] = entity['descriptions']['en']
        entity['claims'] = {
            prop: [value for value in values if value['mainsnak']['datatype'] in accepted_datatypes] for
            prop, values in entity['claims'].items()}
        return json.dumps(entity, ensure_ascii=False) + "\n", entity['id'], title
    except BaseException as error:
        return None, None, None

def claim_value(claim: dict, entity_index: dict):
    if claim["datatype"] == "wikibase-item" and claim["snaktype"] == "value":
        return entity_index.get(claim["datavalue"]["value"]["id"])

def create_data_points_one_input(entity_and_passages, prop_index, entity_index):
    cdef dict entity = entity_and_passages[0]
    cdef list passages = entity_and_passages[1]
    cdef list data_points = []
    cdef str title = entity["labels"]["en"]["value"]
    cdef list claims = [(prop_index[property], claim_value(claim["mainsnak"], entity_index)) for property, prop_claims
                        in
                        entity["claims"].items() if property in prop_index for claim in prop_claims]
    claims = [(prop, value) for (prop, value) in claims if value is not None]
    cdef list positive_indexes
    cdef list positive_passages
    cdef list negative_passages
    for prop, value in claims:
        positive_indexes = [i for i, passage in enumerate(passages) if f" {value.lower()} " in passage.lower()]
        if len(positive_indexes) > 0:
            positive_passages = [passages[i] for i in positive_indexes]
            negative_passages = [passages[i] for i in range(len(passages)) if i not in positive_indexes]
            data_points.append({"question": f"{title} | {prop}", "answers": [value],
                                "positive_ctxs": [{"title": title, "text": pos_passage} for pos_passage in
                                                  positive_passages],
                                "hard_negative_ctxs": [{"title": title, "text": neg_passage} for neg_passage in
                                                       negative_passages]})

    return data_points

def create_data_points(line, df, prop_index, entity_index):
    cdef list data_points = []
    cdef dict entity = json.loads(line)
    cdef str title = entity["labels"]["en"]["value"]
    cdef list claims = [(prop_index[property], claim_value(claim["mainsnak"], entity_index)) for property, prop_claims
                        in
                        entity["claims"].items() if property in prop_index for claim in prop_claims]
    claims = [(prop, value) for (prop, value) in claims if value is not None]
    cdef list passages = list(df[df.title == title].text)
    cdef list positive_indexes
    cdef list positive_passages
    cdef list negative_passages
    for prop, value in claims:
        positive_indexes = [i for i, passage in enumerate(passages) if f" {value.lower()} " in passage.lower()]
        if len(positive_indexes) > 0:
            positive_passages = [passages[i] for i in positive_indexes]
            negative_passages = [passages[i] for i in range(len(passages)) if i not in positive_indexes]
            data_points.append({"question": f"{title} | {prop}", "answers": [value],
                                "positive_ctxs": [{"title": title, "text": pos_passage} for pos_passage in
                                                  positive_passages],
                                "hard_negative_ctxs": [{"title": title, "text": neg_passage} for neg_passage in
                                                       negative_passages]})

    return data_points
