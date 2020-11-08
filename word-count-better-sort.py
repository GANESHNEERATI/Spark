# -*- coding: utf-8 -*-
"""
Created on Sat Feb  8 20:56:44 2020

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

wordCounts=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wordcountsorted=wordCounts.map(lambda x:(x[1],x[0])).sortByKey()
results=wordcountsorted.collect()


for  result in results:
    count=str(result[0])
    word=result[1].encode('ascii','ignore')
    if(word):
        print(word.decode()+":\t\t"+count)
    
