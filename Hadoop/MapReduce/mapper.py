#!/usr/bin/env python3

import sys
import random

for line in sys.stdin:
    # Удаляем символы переноса строки
    line = line.strip()
    if line:
        print(random.random(), str(random.randint(1, 5)) + line, sep="\t")
