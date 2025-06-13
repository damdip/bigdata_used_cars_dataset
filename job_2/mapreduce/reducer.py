#!/usr/bin/env python3
import sys
from collections import defaultdict, Counter

current_key = None
model_set = set()
total_cars = 0
total_days = 0
word_counter = Counter()

def emit_summary(key):
    if total_cars == 0:
        return
    city, year, fascia = key.split("|")
    avg_days = round(total_days / total_cars, 2)
    top_words = [w for w, _ in word_counter.most_common(3)]
    while len(top_words) < 3:
        top_words.append("n/a")
    print(f"{city},{year},{fascia},{len(model_set)},{total_cars},{avg_days},{','.join(top_words)}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        model, count, days, *words = value.strip().split("|")
        count = int(count)
        days = int(days)
        word_list = words[0].split() if words else []

        if key != current_key and current_key is not None:
            emit_summary(current_key)
            # Reset
            model_set = set()
            total_cars = 0
            total_days = 0
            word_counter = Counter()

        current_key = key
        model_set.add(model)
        total_cars += count
        total_days += days
        word_counter.update(word_list)

    except Exception:
        continue

# Emit last
if current_key is not None:
    emit_summary(current_key)