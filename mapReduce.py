import json
import re
from collections import defaultdict

def mapper(text):
    for w in re.findall(r"\b\w+\b", text.lower()):
        yield (w, 1)

def reducer(pairs):
    freq = defaultdict(int)
    for word, count in pairs:
        freq[word] += count
    return freq

def stream_jsonl(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except:
                continue

def run_mapreduce(file_path, text_key="text"):
    mapped = (
        pair
        for obj in stream_jsonl(file_path)
        if text_key in obj and isinstance(obj[text_key], str)
        for pair in mapper(obj[text_key])
    )
    reduced = reducer(mapped)
    sorted_items = sorted(reduced.items(), key=lambda x: x[1], reverse=True)
    print("\nTop 20 words:")
    for w, c in sorted_items[:20]:
        print(w, c)
    return sorted_items

if __name__ == "__main__":
    run_mapreduce("dataset.json")
