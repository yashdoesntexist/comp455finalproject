import json
import re
import os
from collections import defaultdict

def mapper(text):
    for w in re.findall(r"\b\w+\b", text.lower()):
        yield (w, 1)

def reducer(pairs):
    freq = defaultdict(int)
    for word, count in pairs:
        freq[word] += count
    return freq

def stream_jsonl(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except:
                continue

def iter_all_wiki_files(root_folder):
    for folder, subdirs, files in os.walk(root_folder):
        for name in files:
            if name.startswith("wiki_"):
                yield os.path.join(folder, name)

def run_mapreduce(root_folder):
    mapped = (
        pair
        for file_path in iter_all_wiki_files(root_folder)
        for obj in stream_jsonl(file_path)
        if "text" in obj and isinstance(obj["text"], str)
        for pair in mapper(obj["text"])
    )
    reduced = reducer(mapped)
    sorted_items = sorted(reduced.items(), key=lambda x: x[1], reverse=True)
    print("\nTop 20 words:")
    for word, count in sorted_items[:20]:
        print(word, count)
    return sorted_items

if __name__ == "__main__":
    run_mapreduce("DATA")
