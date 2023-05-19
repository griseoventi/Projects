#!/usr/bin/env python3

import sys

d = {'1': [], '2': [], '3': [], '4': [], '5': []}
for line in sys.stdin:
    # Удаляем символы переноса строки
    cansel, group_id = line.strip().split('\t')
    group, a_id = group_id[0], group_id[1:]
    d[group] = d.get(group, []) + [a_id]
    if len(d[group]) == int(group):
        print(*d[group], sep=',')
        d[group].clear()

for i in range(1, 6):
    if len(d[str(i)]) != 0:
        print(*d[str(i)], sep=',')
