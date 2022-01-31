import json

from processing_utils import process

if __name__ == '__main__':

    index = process()
    json.dump(index, open("data/entity_index.json", 'w'), ensure_ascii=False, indent=2)