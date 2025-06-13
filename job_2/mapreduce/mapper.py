#!/usr/bin/env python3
import sys
import csv
import re

STOPWORDS = {"the", "and", "for", "with", "you", "are", "your", "this", "that", "have",
             "from", "but", "not", "all", "can", "has", "was", "any", "out", "our", "new",
             "use", "get", "one", "off", "on", "in", "at", "a", "an", "tn", "to", "of", "by", "as", "it", "is"}

def clean_and_tokenize(text):
    words = re.findall(r'\b\w{4,}\b', text.lower())
    return [word for word in words if word not in STOPWORDS]

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        city = row[0].strip()
        year = row[1].strip()
        price = float(row[2])
        model_name = row[3].strip()
        daysonmarket = int(row[4]) if row[4] else 0
        description = row[5] if len(row) > 5 else ""

        if price > 50000:
            fascia = "Alta"
        elif price >= 20000:
            fascia = "Media"
        else:
            fascia = "Bassa"

        key = f"{city}|{year}|{fascia}"
        words = clean_and_tokenize(description)
        print(f"{key}\t{model_name}|1|{daysonmarket}|{' '.join(words)}")
    except Exception:
        continue