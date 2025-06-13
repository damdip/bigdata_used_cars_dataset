#!/usr/bin/env python3

import argparse
import time
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, StringType

# === Timer inizio ===
start_time = time.time()

# === Argomenti da linea di comando ===
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input CSV file path")
parser.add_argument("--output_path", type=str, help="Output folder path")
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# === Inizializza Spark ===
spark = SparkSession.builder \
    .appName("CarPriceReportJob-SparkSQL") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()

# === Legge CSV con header e tipi ===
df = spark.read.option("header", True).csv(input_filepath)

# === Casting colonne fondamentali ===
df = df.withColumn("year", F.col("year").cast(IntegerType())) \
       .withColumn("price", F.col("price").cast(FloatType())) \
       .withColumn("daysonmarket", F.col("daysonmarket").cast(IntegerType()))

# === Aggiunge colonna fascia prezzo ===
df = df.withColumn(
    "fascia",
    F.when(F.col("price") > 50000, "alta")
     .when(F.col("price") >= 20000, "media")
     .otherwise("bassa")
)

# === Crea vista temporanea per SQL ===
df.createOrReplaceTempView("cars")

# === Query SQL di aggregazione ===
# 1) Numero modelli distinti
# 2) Conteggio totale auto
# 3) Media daysonmarket
# 4) Estrazione top 3 parole descrizione (con UDF Python sotto)

# Funzione UDF per top 3 parole da descrizioni concatenate
from pyspark.sql.types import ArrayType

def top3_words_concat(descs):
    from collections import Counter
    import re

    # Stopwords base (puoi estendere la lista se vuoi)
    stopwords = set([
        "the", "and", "for", "with", "that", "this", "you", "your", "from", "are",
        "was", "have", "has", "but", "not", "all", "can", "will", "more", "one",
        "our", "any", "its", "new", "low", "high", "top", "out", "get", "own", "off"
    ])

    if descs is None:
        return []

    # Tokenizza parole (solo alfanumeriche), normalizza lowercase
    words = re.findall(r'\b\w+\b', descs.lower())

    # Filtra parole: rimuove stopwords e parole con ≤3 lettere
    filtered = [w for w in words if len(w) > 3 and w not in stopwords]

    counter = Counter(filtered)
    return [w for w, _ in counter.most_common(3)]


top3_udf = F.udf(top3_words_concat, ArrayType(StringType()))

# 1) Raggruppa e concatena descrizioni con separator (es. uno spazio)
agg_df = spark.sql("""
SELECT
  city,
  year,
  fascia,
  COUNT(DISTINCT model_name) AS num_modelli,
  COUNT(*) AS num_auto,
  AVG(daysonmarket) AS avg_daysonmarket,
  CONCAT_WS(' ', COLLECT_LIST(description)) AS all_descriptions
FROM cars
GROUP BY city, year, fascia
""")

# 2) Calcola top 3 parole con UDF
agg_df = agg_df.withColumn("top3_words", top3_udf("all_descriptions"))

# 3) Trasforma lista top3 in stringa CSV
agg_df = agg_df.withColumn("top3_str", F.array_join("top3_words", ","))

# 4) Seleziona solo le colonne richieste
result_df = agg_df.select(
    "city", "year", "fascia",
    "num_modelli", "num_auto",
    F.round("avg_daysonmarket", 2).alias("avg_daysonmarket"),
    "top3_str"
)

# === Salvataggio risultato ===
result_df.write.mode("overwrite").csv(output_filepath, header=True)

# === Debug e timer ===
output_count = result_df.count()
print(f"[RESULT] Numero record finali: {output_count}", file=sys.stderr)
print(f"[TIMER] Tempo totale: {time.time() - start_time:.2f} secondi", file=sys.stderr)

# === Stop ===
spark.stop()