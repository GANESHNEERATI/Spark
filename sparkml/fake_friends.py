# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 14:18:40 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("friendsbyage")
sc = SparkContext(conf = conf)

def  parseLine(line):
    fields=line.split(',')
    age=int(fields[2])
    numfriends=int(fields[3])
    return (age,numfriends)

lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/fakefriends.csv")
rdd=lines.map(parseLine)
totalByAge=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda  x,y:(x[0]+y[0],x[1]+y[1]))
avgByAge=totalByAge.mapValues(lambda x:x[0]/x[1])
results=avgByAge.collect()



for result in results:
    print(result)
    