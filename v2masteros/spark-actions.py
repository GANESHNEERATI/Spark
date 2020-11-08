# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 21:08:54 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf = conf)

autodata=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/v2-masteros/auto-data.csv")

colldata=sc.parallelize([4,5,6,7,9])

"""sum=colldata.reduce(lambda x,y:x+y)
print(sum)

numoflines=autodata.reduce(lambda x,y:x if len(x)<len(y) else  y)

print(numoflines) """


def getMPG(autostr):
    if isinstance(autostr,int):
        return autostr
    attrlist=autostr.split(",")
    if attrlist[9].isdigit():
        return int(attrlist[9])
    else:
        return 0;
    


avgmpg=autodata.reduce(lambda x,y:getMPG(x)+getMPG(y)) \
    /(autodata.count()-1)
    
print(avgmpg)
    

