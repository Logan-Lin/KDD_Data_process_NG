# -*- coding:utf-8 -*-
import json

with open('data.txt', 'r') as f:
    data = [json.loads(i.strip()) for i in f.readlines()]
    for d in data:
        print(d['time'])
        for row in d['data']:
            print(row)
        print('\n')
