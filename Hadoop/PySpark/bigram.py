#!/usr/bin/env python3

from pyspark import SparkConf, SparkContext

spark_conf = ( 
    SparkConf()
    .set("spark.ui.port", -)
    .set("spark.driver.memory", "512m")
    .set("spark.executor.instances", "2")
    .set("spark.executor.cores", "1")
    .setAppName("your shiny name")
    .setMaster("yarn")
)
sc = SparkContext(conf=spark_conf)

import re
articles = sc.textFile("hdfs:///data/wiki/en_articles_part")

# Разделим на id и остальной текст
# Здесь мы сначала используем flatMap для получения всех последовательных пар слов в каждой строке. 
# Затем мы фильтруем только те пары слов, где первое слово равно "narodnaya". 
# Далее мы используем map и reduceByKey для подсчета количества вхождений каждой пары слов
 
word_pairs = articles.map(lambda x: x.split('\t')[1]) \
                     .flatMap(lambda x: [(re.sub(r'[^\w\s]','',word.lower()), re.sub(r'[^\w\s]','',next_word.lower())) for word, next_word in zip(x.split()[:-1], x.split()[1:])]) \
                     .filter(lambda x: x[0] == 'narodnaya')


frequency = word_pairs \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortByKey()

for pair, freq in frequency.collect():
    print(f"{pair[0]}_{pair[1]}\t{freq}")

sc.stop()
