# -*- coding: utf-8 -*-
"""
Created on Sat Feb  8 20:17:04 2020

@author: NEERATI GANESH
"""
import re
from pyspark import SparkConf, SparkContext
import collections

def normalizetext(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf = conf)

lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/Book.txt")
words=lines.flatMap(normalizetext)

wordCounts=words.countByValue()

for  word,count in wordCounts.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword,count) 
