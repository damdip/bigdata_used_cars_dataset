#!usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, collect_list, count, min, max, avg, struct, round as spark_round

def main(input_path, output_path, output_format):
    # 1. Avvia Spark
    spark = SparkSession.builder \
        .appName("AutoStatsByMakeAndModel") \
        .getOrCreate()
    

    # 2. Leggi il dataset (presunto CSV con intestazioni)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # 3. Vista temporanea per usare SQL
    df.createOrReplaceTempView("cars")

    # 4. Query SQL per aggregare statistiche per marca e modello
    query = """
    SELECT
      make_name,
      model_name,
      COUNT(*) AS car_count,
      MIN(price) AS min_price,
      MAX(price) AS max_price,
      ROUND(AVG(price), 2) AS avg_price,
      COLLECT_SET(year) AS years_present
    FROM cars
    GROUP BY make_name, model_name
    """
    model_stats_df = spark.sql(query)

    # 5. Raggruppamento finale per marca con lista modelli
    result_df = model_stats_df \
        .withColumn("model_info", struct("model_name", "car_count", "min_price", "max_price", "avg_price", "years_present")) \
        .groupBy("make_name") \
        .agg(collect_list("model_info").alias("models"))
    

    # 6. Scrittura dell'output nel formato specificato
    result_df.write.mode("overwrite").format(output_format).save(output_path)

    
    # 7. Chiudi la sessione
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calcolo statistiche auto per marca e modello con Spark SQL.")
    parser.add_argument("--input", required=True, help="Percorso input del dataset (es. HDFS o locale)")
    parser.add_argument("--output", required=True, help="Percorso di output per i risultati")
    parser.add_argument("--format", default="json", choices=["json", "csv", "parquet"], help="Formato output (default: json)")

    args = parser.parse_args()
    main(args.input, args.output, args.format)
