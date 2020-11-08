# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 16:43:56 2020

@author: NEERATI GANESH
"""

from pyspark.sql import  SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark=SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/temp" ).appName("sparkSQL").getOrCreate()


def loadmovies():
    movienames={}
    with open("D:/D/SOFTWARES/HADOOP/datasets/ml-100k/u.item") as f:
        for line in f:
            fields=line.split('|')
            movienames[int(fields[0])]=fields[1]
    
    return movienames

namedict=loadmovies()

lines=spark.sparkContext.textFile("D:/D/SOFTWARES/HADOOP/datasets/ml-100k/u.data")

movies=lines.map(lambda x:Row(movieid=int(x.split()[1])))

moviedataset=spark.createDataFrame(movies)
topmovieids=moviedataset.groupBy("movieid").count().orderBy("count",ascending=False).cache()

topmovieids.show()

top10=topmovieids.take(10)

print("\n")

for result in top10:
    print(namedict[result[0]],result[1])
    
    
spark.stop()



            