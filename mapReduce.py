import json
import re
from collections import defaultdict

def mapper(text):
    words = re.findall(r'\b\w+\b', text.lower())
    return [(word, 1) for word in words]

def reducer(mapped_data):
    reduced = defaultdict(int)
    for word, count in mapped_data:
        reduced[word] += count
    return reduced

def sort_by_frequency(reduced_data):
    return sorted(reduced_data.items(), key=lambda x: x[1], reverse=True)

def run_mapreduce_from_json(file_path, text_key="content"):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    all_mapped = []
    for record in data:
        if text_key in record and isinstance(record[text_key], str):
            all_mapped.extend(mapper(record[text_key]))
    reduced = reducer(all_mapped)
    sorted_data = sort_by_frequency(reduced)
    print("\nTop 20 Most Frequent Words:")
    for word, freq in sorted_data[:20]:
        print(f"{word}: {freq}")
    return sorted_data

if __name__ == "__main__":
    json_file = "dataset.json"
    run_mapreduce_from_json(json_file, text_key="content")
