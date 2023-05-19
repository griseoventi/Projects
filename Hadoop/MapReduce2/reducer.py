#!/usr/bin/env python3

import sys

d = {}

for line in sys.stdin:
    # Удаляем символы переноса строки
    year, tag, counts = line.strip().split('\t')
    if year + tag in d:
        d[year + tag] += int(counts)
    else:
        d[year + tag] = int(counts)

for key, value in d.items():
    print(key[:4], key[4:], value, sep="\t")
