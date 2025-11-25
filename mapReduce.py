import json
import re
import os
from collections import defaultdict
import boto3
from dotenv import load_dotenv

load_dotenv()

def mapper(text):
    for token in re.findall(r"\b\w+\b", text.lower()):
        yield (token, 1)

def reducer(counts, pairs):
    for word, value in pairs:
        counts[word] += value
    return counts

def stream_jsonl_s3(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    for line in obj["Body"].iter_lines():
        if not line:
            continue
        try:
            yield json.loads(line)
        except:
            continue

def iter_s3_files(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "wiki_" in key:
                yield key

def run_mapreduce_s3(bucket, prefix):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    global_counts = defaultdict(int)

    for key in iter_s3_files(s3, bucket, prefix):
        print(f"Processing: {key}")
        mapped_pairs = (
            pair
            for record in stream_jsonl_s3(s3, bucket, key)
            if "text" in record and isinstance(record["text"], str)
            for pair in mapper(record["text"])
        )
        reducer(global_counts, mapped_pairs)

    return sorted(global_counts.items(), key=lambda x: x[1], reverse=True)

if __name__ == "__main__":
    bucket = "wikidocs455"
    prefix = ""   

    results = run_mapreduce_s3(bucket, prefix)

    print("\nTop 50 words:")
    for word, count in results[:50]:
        print(word, count)
