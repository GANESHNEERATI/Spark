# -*- coding: utf-8 -*-
"""
Created on Sat Jan 25 22:02:46 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf = conf)


lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/Book.txt")

words=lines.flatMap(lambda x:x.split())

wordCounts=words.countByValue()

for  word,count in wordCounts.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword,count)