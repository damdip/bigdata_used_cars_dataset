#!/usr/bin/env python3
import sys

from collections import defaultdict

current_key = None
prices = []
years = set()

def emit_stats(key, prices, years):
    if not prices:
        return
    make, model = key.split('|')
    print(f"{make}\t{model}\t{len(prices)}\t{min(prices)}\t{max(prices)}\t{sum(prices)/len(prices):.2f}\t{','.join(sorted(years))}")

for line in sys.stdin:
    try:
        key, value = line.strip().split('\t')
        price_str, year = value.split(',')

        price = float(price_str)

        if key != current_key:
            if current_key:
                emit_stats(current_key, prices, years)
            current_key = key
            prices = []
            years = set()

        prices.append(price)
        years.add(year)
    except Exception:
        continue

# Emit last key
if current_key:
    emit_stats(current_key, prices, years)