import json
import time
from multiprocessing import Pool

from processing_utils import line_process

ENTITIES_INDEX = "data/entity_index.json"
ENTITIES_FILE = "data/entities.jsonl"

if __name__ == '__main__':
    start = time.time()
    index = {}

    with open("data/latest-all.json", "r") as f, open(ENTITIES_FILE, 'w') as g:
        with Pool(90) as p:
            for i, (json_entity, entity_id, entity_labels) in enumerate(p.imap(line_process, f)):
                if json_entity is not None:
                    g.write(json_entity)
                    index[entity_id] = entity_labels
                if i % 10000000 == 0 and i > 0:
                    print(i, "{:.4f}".format(time.time() - start))
                    json.dump(index, open(ENTITIES_INDEX, 'w'), ensure_ascii=False, indent=2)
    json.dump(index, open(ENTITIES_INDEX, 'w'), ensure_ascii=False, indent=2)
