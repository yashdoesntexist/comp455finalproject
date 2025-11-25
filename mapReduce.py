from pyspark.sql import SparkSession
import re
import json

def tokenize(t):
    return re.findall(r"\b\w+\b", t.lower())

def mapper(record):
    text = record.get("text", "")
    for w in tokenize(text):
        yield (w, 1)

def parse_json(line):
    try:
        obj = json.loads(line)
        return {"text": obj.get("text", "")}
    except:
        return {"text": ""}

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MapReduceWordCount").getOrCreate()
    sc = spark.sparkContext

    bucket = "wikidocs455"
    prefix = ""
    path = f"s3a://{bucket}/{prefix}wiki_*"

    rdd = sc.textFile(path)
    parsed = rdd.map(parse_json)
    mapped = parsed.flatMap(mapper)
    reduced = mapped.reduceByKey(lambda a, b: a + b)
    sorted_result = reduced.sortBy(lambda x: -x[1])

    sorted_result.saveAsTextFile("s3a://wikidocs455/output_wordcount")

    spark.stop()
