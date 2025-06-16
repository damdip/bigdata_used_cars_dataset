#!/usr/bin/env python3

import argparse
import time
import sys
import csv
from io import StringIO
from collections import Counter
from pyspark.sql import SparkSession

# === Timer inizio ===
start_time = time.time()

# === Argomenti da linea di comando ===
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# === Inizializza Spark ===
spark = SparkSession.builder \
    .appName("CarPriceReportJob-RDD") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()

sc = spark.sparkContext

# === Funzione per fascia prezzo ===
def price_fascia(price):
    if price > 50000:
        return "alta"
    elif price >= 20000:
        return "media"
    else:
        return "bassa"

# === Parsing CSV sicuro ===
def parse_line(line):
    try:
        reader = csv.reader(StringIO(line))
        parts = next(reader)
        city = parts[0].strip()
        year = int(parts[1].strip())
        price = float(parts[2].strip())
        model = parts[3].strip()
        daysonmarket = int(parts[4].strip())
        description = parts[5].strip().strip('"')
        fascia = price_fascia(price)
        return ((city, year, fascia), (model, 1, daysonmarket, description))
    except Exception as e:
        print(f"[PARSE ERROR] {e} on line: {line}", file=sys.stderr)
        return None

# === Lettura file ===
rdd_raw = sc.textFile(input_filepath)
header = rdd_raw.first()
rdd_data = rdd_raw.filter(lambda row: row != header)

original_count= rdd_data.count()

print(f"[DEBUG] Numero righe originali: {original_count}", file=sys.stderr)

rdd_parsed = rdd_data.map(parse_line).filter(lambda x: x is not None)

parsed_count = rdd_parsed.count()
print(f"[DEBUG] Righe parse ok: {parsed_count}", file=sys.stderr)

# === Aggregazione ===
def seq_func(acc, value):
    models, count, days_sum, descriptions = acc
    model, c, days, desc = value
    models.add(model)
    count += c
    days_sum += days
    descriptions.append(desc)
    return (models, count, days_sum, descriptions)

def comb_func(acc1, acc2):
    m1, c1, d1, desc1 = acc1
    m2, c2, d2, desc2 = acc2
    return (m1.union(m2), c1 + c2, d1 + d2, desc1 + desc2)

zero = (set(), 0, 0, [])
rdd_agg = rdd_parsed.aggregateByKey(zero, seq_func, comb_func)

agg_count = rdd_agg.count()
print(f"[DEBUG] Numero gruppi aggregati: {agg_count}", file=sys.stderr)
stopwords = set([
            "the", "and", "for", "with", "that", "this", "you", "your", "from", "are",
        "was", "have", "has", "but", "not", "all", "can", "will", "more", "one",
        "our", "any", "its", "new", "low", "high", "top", "out", "get", "own", "off"
       
    ])

# === Calcolo top 3 parole ===
import re
from collections import Counter

def compute_top3_words(desc_list, min_word_length=4):
    
    stopwords = {
        "the", "and", "for", "with", "that", "this", "you", "your", "from", "are",
        "was", "have", "has", "but", "not", "all", "can", "will", "more", "one",
        "our", "any", "its", "new", "low", "high", "top", "out", "get", "own", "off"
    }

    words = []
    for desc in desc_list:
        tokens = re.findall(r'\b[a-z]+\b', desc.lower())
        filtered = [w for w in tokens if w not in stopwords and len(w) >= 4]
        words.extend(filtered)

    counter = Counter(words)
    top3 = [word for word, _ in counter.most_common(3)]
    return top3

# === Format finale ===
def format_output(record):
    (city, year, fascia), (model_set, num_auto, days_sum, desc_list) = record
    num_modelli = len(model_set)
    media_days = days_sum / num_auto if num_auto > 0 else 0.0
    top3_words = compute_top3_words(desc_list)
    top3_str = ",".join(top3_words)
    return f"{city},{year},{fascia},{num_modelli},{num_auto},{media_days:.2f},{top3_str}"


rdd_result = rdd_agg.map(format_output)

# === Conteggio finale dei record ===
output_count = rdd_result.count()

# === Salvataggio su HDFS ===
rdd_result.saveAsTextFile(output_filepath)

# === Stop ===
spark.stop()