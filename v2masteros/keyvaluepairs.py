# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 15:00:41 2020

@author: NEERATI GANESH
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf = conf)

autodata=sc.textFile("D:/D/SOFTWARES/HADOOP/datasets/v2-masteros/auto-data.csv")



cyldata=autodata.map(lambda x:(x.split(",")[0],x.split(",")[7]))



print(cyldata.take(5))


#removing hedders

hedder=cyldata.first()
cylHpdata=cyldata.filter(lambda line: line  != hedder)

addone=cylHpdata.mapValues(lambda x:(x,1))
aa=addone.collect()

brandvalues=aa \
    .reduceByKey(lambda x,y:(int(x[0])+int(y[0]),\
        x[1]+y[1]))
        

result=brandvalues.collect()

r=result.mapValues(lambda x:int(x[0])/int(x[1]))
r1= r.collect()

for brand,value in r1:
    print(brand,value)




        
        
