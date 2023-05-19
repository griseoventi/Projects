#!/usr/bin/env python3

import sys

count = 0

for line in sys.stdin:
    # Удаляем символы переноса строки
    year, tag, counts = line.strip().split('\t')
    if year == '2010' and count < 10:
        print(year, tag, counts, sep='\t')
        count += 1
    if year == '2016' and 10 <= count < 20:
        print(year, tag, counts, sep='\t')
        count += 1
