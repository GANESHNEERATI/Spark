# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 21:02:08 2020

@author: NEERATI GANESH
"""

from pyspark import SparkContext,SparkConf

sc=SparkContext.getOrCreate()

autodata=sc.textFile("D:/movies/SparkPythonDoBigDataAnalytics-Resources/auto-miles-per-gallon.csv")

autodata.cache()
autodata.take(5)

dataLiness=autodata.filter(lambda x:"CYLINDERES" not in x )
dataLiness.count()

from pyspark.sql import Row
avgHp=sc.broadcast(80.0)

def cleanUp( inputstr):
    global avgHp
    attList=inputstr.split(",")
    hpvalue=attList[3]
    if hpvalue =='?':
        hpvalue=avgHp.value
    
    values=Row(MPG=float(attList[0]),\
               CYLINDERS=float(attList[1]),\
               DISPLACEMENT=float(attList[2]),\
               HORSEPOWER=float(hpvalue),\
               WEIGHT=float(attList[4]),\
               ACCELERATION=float(attList[5]),\
               MODELYEAR=float(attList[6]),\
               NAME=attList[7])
    return values


autoMap=dataLiness.map(cleanUp)
autoMap.cache()
autoMap.take(5)
            
               
        
    
    


