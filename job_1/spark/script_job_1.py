#!usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.utils import StreamingQueryException

# Listener per ottenere metriche
from pyspark import TaskContext


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath,output_filepath = args.input_path, args.output_path

# Crea la SparkSession
spark = SparkSession.builder \
    .appName("Job1") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()

# Ottieni lo SparkContext dalla sessione
sc = spark.sparkContext

# Leggi il file CSV da HDFS
input_RDD = sc.textFile(input_filepath)

# Estrai intestazione
header = input_RDD.first()

# Rimuovi intestazione
data_rdd = input_RDD.filter(lambda row: row != header)

# Parse dei record (split su virgola, con gestione base di valori numerici)
def parse_line(line):
    try:
        parts = line.split(",")
        make = parts[0].strip()
        model = parts[1].strip()
        price = float(parts[2])
        year = int(parts[3])
        return ((make, model), (1, price, price, price, set([year])))
    except:
        return None

parsed_rdd = data_rdd.map(parse_line).filter(lambda x: x is not None)

# Aggrega per (make, model)
def reduce_func(a, b):
    count = a[0] + b[0]
    min_price = min(a[1], b[1])
    max_price = max(a[2], b[2])
    total_price = a[3] + b[3]
    years = a[4].union(b[4])
    return (count, min_price, max_price, total_price, years)

aggregated_rdd = parsed_rdd.reduceByKey(reduce_func)

# Calcola la media e formatta i risultati
def format_result(record):
    (make, model), (count, min_price, max_price, total_price, years) = record
    avg_price = total_price / count
    years_list = sorted(list(years))
    return f"{make},{model},{count},{min_price:.2f},{max_price:.2f},{avg_price:.2f},{'|'.join(map(str, years_list))}"

result_rdd = aggregated_rdd.map(format_result)

# Salva su HDFS
result_rdd.saveAsTextFile(output_filepath)


print(f"Numero record risultato: {result_rdd.count()}")