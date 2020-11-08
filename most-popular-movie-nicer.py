# -*- coding: utf-8 -*-
"""
Created on Sun Feb  9 12:41:22 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext


def loadmovienames():
    movienames={}
    with open("D:/D/SOFTWARES/HADOOP/datasets/ml-100k/u.data") as f:
        for line in f:
         fields=line.split('|')
         movienames[int(fields[0])]=fields[1]
    return movienames


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

nameDict=sc.broadcast(loadmovienames())
lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/ml-100k/u.data")

movies=lines.map(lambda x:(int(x.split()[1]),1))
moviesbycount=movies.reduceByKey(lambda x,y:x+y)
flipped=moviesbycount.map(lambda  x:(x[1],x[0]))
sortedmovies=flipped.sortByKey()

sortedmovieswithnames=sortedmovies.map(lambda countmovie:(nameDict.value[countmovie[1]],countmovie[0]))

results=sortedmovieswithnames.collect()

for result in results:
    print(result)
        