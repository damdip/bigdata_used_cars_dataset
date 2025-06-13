#!/usr/bin/env python3
import sys

# Salta l'intestazione
header = True

for line in sys.stdin:
    if header:
        header = False
        continue

    try:
        parts = line.strip().split(',')
        if len(parts) < 4:
            continue  # salta righe malformate

        make = parts[0].strip()
        model = parts[1].strip()
        price = float(parts[2].strip())
        year = parts[3].strip()

        key = f"{make}|{model}"
        value = f"{price},{year}"

        print(f"{key}\t{value}")
    except Exception:
        continue  # ignora errori di parsing