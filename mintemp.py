# -*- coding: utf-8 -*-
"""
Created on Sat Jan 25 21:12:30 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("mintemp")
sc = SparkContext(conf = conf)


def parseLine(line):
     fields=line.split(',')
     stationId=fields[0]
     entryType=fields[2]
     temprature=float(fields[3])*0.1*(9.0/5.0)+32.0
     return(stationId,entryType,temprature)
 
    
lines=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/1800.csv")
    
parseLines=lines.map(parseLine)

minTemp=parseLines.filter(lambda x:"TMIN" in x[1])

stationTemp=minTemp.map(lambda x:(x[0],x[2]))

minTemp=stationTemp.reduceByKey(lambda x,y:min(x,y))

results=minTemp.collect()

for result in results:
    print(result[0]+ "\t{:.2f}F".format(result[1]))






     
     
