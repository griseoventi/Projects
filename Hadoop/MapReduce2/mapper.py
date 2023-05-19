#!/usr/bin/env python3

import sys
import re
from collections import Counter

for line in sys.stdin:
    # Удаляем символы переноса строки
    line = line.strip()
    if line and '<row' in line and 'Tags="' in line and 'CreationDate="' in line:
        # Извлекаем год из даты создания поста
        year = re.findall('CreationDate="(.*?)-', line)[0]
        tags_str = re.search(r'Tags="(.+?)"', line).group(1)
        if year in ("2010", "2016") and tags_str:
            ags_list = Counter(re.findall(r'&lt;([\w#]+)&gt;', tags_str))
            for key, value in ags_list.items():
                print(year, key, value, sep="\t")
