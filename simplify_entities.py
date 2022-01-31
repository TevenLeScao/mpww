import argparse
import json


def claim_value(claim: dict, entity_index: dict):
    if claim["datatype"] == "wikibase-item" and claim["snaktype"] == "value":
        return entity_index.get(claim["datavalue"]["value"]["id"])
    if claim["datatype"] == "quantity" and claim["snaktype"] == "value":
        # stripping off the leading + or -
        return entity_index.get(claim["datavalue"]["value"]["amount"][1:])
    if claim["datatype"] == "time" and claim["snaktype"] == "value":
        time_value = claim["datavalue"]["value"]["time"]
        # those are the dates
        if time_value[5:] == "-01-01T00:00:00Z":
            time_value = time_value[1:5]
        else:
            time_value = None
        return time_value



def simplify_entity(entity, prop_index, entity_index):
    try:
        aliases = [alias["value"] for alias in entity["aliases"].get("en", [])]
        title = entity["labels"]["en"]["value"]
        description = entity["descriptions"]["value"] if entity["descriptions"]["language"] == "en" else None
        claims = [(prop_index[property], claim_value(claim["mainsnak"], entity_index))
                  for property, prop_claims in entity["claims"].items() if property in prop_index
                  for claim in prop_claims if claim_value(claim["mainsnak"], entity_index) is not None]
        return {"alias": aliases, "title": title, "description": description, "claims": claims}
    except KeyError:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_file", type=str, default="entities.jsonl")
    parser.add_argument("--out_file", type=str, default="simplified_entities.jsonl")
    parser.add_argument("--entities_index", type=str, required=True)
    parser.add_argument("--prop_index", type=str, required=True)
    args = parser.parse_args()
    with open(args.entities_index) as entity_index_file, open(args.prop_index) as prop_index_file:
        entity_index = json.load(entity_index_file)
        prop_index = json.load(prop_index_file)
    with open(args.in_file) as f, open(args.out_file, "w") as g:
        for line in f:
            simplified = simplify_entity(json.loads(line[:-1]), prop_index, entity_index)
            g.write(json.dumps(simplified, ensure_ascii=False) + "\n")
