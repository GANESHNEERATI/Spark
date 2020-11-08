# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 20:39:13 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf = conf)

autodata=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/v2-masteros/auto-data.csv")


colldata=sc.parallelize([4,5,4,3,2])

"""
transformations


tsvData=autodata.map(lambda x:x.replace(",","\t"))

print(tsvData.take(5))  """

#for filter
"""
toyotaData=autodata.filter(lambda x:"toyota" in x)
print(toyotaData.count())

"""

#flat map
"""
words=autodata.flatMap(lambda x:x.split(","))

count=words.count()
print(count)
print(words.take(20))

"""
#distnict

"""

for  numdata in colldata.distinct().collect():
    print(numdata)
    
    
"""

#set operations

words1=sc.parallelize(['hai','hello','welocme','nothing'])
words2=sc.parallelize(['hai','ganesh','preetam'])


for unions in words1.union(words2).distinct().collect():
    print(unions)
    
    
print('for intersection')

for intersect in words1.intersection(words2).collect():
    print(intersect)


