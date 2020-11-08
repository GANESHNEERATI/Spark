# -*- coding: utf-8 -*-
"""
Created on Sun Feb  9 11:51:32 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/ml-100k/u.data")

movies=lines.map(lambda x:(int(x.split()[1]),1))
moviesbycount=movies.reduceByKey(lambda x,y:x+y)

flipped=moviesbycount.map(lambda  x:(x[1],x[0]))

sortedmovies=flipped.sortByKey()

results=sortedmovies.collect()

for result in results:
    print(result)




