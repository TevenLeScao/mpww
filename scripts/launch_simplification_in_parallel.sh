for i in {00..59}
do python dataset/simplify_entities.py --in_file raw_data/entities_files/split_${i} --out_file raw_data/simplified_entities_files/simplified_${i} --entities_index raw_data/entity_index.json --prop_index raw_data/props.json &
done
wait
