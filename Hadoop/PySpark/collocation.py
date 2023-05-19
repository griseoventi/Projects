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
import math

# Загружаем стоп-слова
stop_words = sc.textFile("hdfs:///data/stop_words/stop_words_en-xpo6.txt")
stop_words_broadcast = sc.broadcast(stop_words.collect())

# Загружаем статьи Википедии
wiki = sc.textFile("hdfs:///data/wiki/en_articles_part")

# Выделяем слова из статей и приводим их к нижнему регистру 
wiki_words = wiki.map(lambda x: x.split('\t')[1]) \
                 .flatMap(lambda x: re.findall(r"\w+",re.sub(r'[^\w\s]','',x.lower())))


# Удаляем стоп-слова
wiki_words_filtered = wiki_words.filter(lambda x: x not in stop_words_broadcast.value)


wiki_words_filtered_count = wiki_words_filtered.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)


# Функция для удаления стоп-слов
def remove_stop_words(x):
    return [word for word in x if word not in stop_words_broadcast.value]

# Применяем re.findall к каждой строке из RDD 'wiki' и разделяем строки по пробелам
wiki_words = wiki.map(lambda x: x.split('\t')[1]) \
                 .map(lambda x: re.findall(r"\w+",re.sub(r'[^\w\s]','',x.lower()))) \
                 .map(remove_stop_words) \
                 .flatMap(lambda x: [f"{x[i]}_{x[i+1]}" for i in range(len(x)-1)])

# посчитаем общее кол-во биграмм 
full_count = wiki_words.count()

# Вычисляем частоты биграмм
wiki_bigrams_count = wiki_words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wiki_words_count_500 = wiki_bigrams_count.filter(lambda x: int(x[1]) > 500)
frequent_bigrams = wiki_bigrams_count.map(lambda x: x[0])

# P(a) - вероятность увидеть слово “a” в датасете.
# P(a) = num_of_occurrences_of_word_"a" / total_number_of_bigrams
# total_number_of_bigrams - общее количество биграмм (пар идущих подряд слов) в тексте

P_a = wiki_words_filtered_count.map(lambda x: (x[0], x[1]/full_count))
P_b = wiki_words_filtered_count.map(lambda x: (x[0], x[1]/full_count))

# P(ab) - вероятность увидеть пару слов “a” и “b”, идущих подряд.
# P(ab) = num_of_occurrences_of_pair_"ab" / total_number_of_bigrams
# total_number_of_bigrams - общее количество биграмм

P_ab = wiki_words_count_500.map(lambda x: (x[0], x[1]/full_count))

# PMI(a,b) = ln( P(ab) / [P(a) x P(b)] )
# Присоединим RDD с частотами слов к RDD с частотами биграмм по первому слову в биграмме ('это наш топ 500 биграмм'):
# Присоединим RDD с частотами слов к результату из предыдущего шага по второму слову в биграмме:
joined_rdd = frequent_bigrams.map(lambda x: (x.split('_')[0], (x.split('_')[0], x.split('_')[1]))).join(P_a)
joined_rdd = joined_rdd.map(lambda x: (x[1][0][1], (x[1][0][0], x[1][0][1], x[1][1]))).join(P_b)
joined_rdd = joined_rdd.map(lambda x: (x[1][0][0] + '_' + x[1][0][1], (x[1][0][2], x[1][1]))).join(P_ab)
joined_rdd = joined_rdd.map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])))

# PMI(a,b) = ln( P(ab) / [P(a) x P(b)] )
# Вычислим PMI для каждой биграммы

PMI_a_b = joined_rdd.map(lambda x: (x[0], math.log(x[1][2] / (x[1][0] * x[1][1])), x[1][2]))

# Вычислите NPMI для каждой биграммы
# NPMI(a,b) = PMI(a,b) / -ln(P(ab))
def npmi(pmi, p_ab):
    return pmi / (-math.log(p_ab))

NPMI = PMI_a_b.map(lambda x: (x[0], round(x[1] / -math.log(x[2]), 3)))
NPMI_39 = NPMI.takeOrdered(39, key=lambda x: -x[1])
for i in NPMI_39:
    print(i[0], i[1], sep='\t')

sc.stop()

