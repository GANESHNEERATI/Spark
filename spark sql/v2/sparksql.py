# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 18:50:50 2020

@author: NEERATI GANESH
"""

from pyspark.sql import  SparkSession
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext

spark=SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/temp" ).appName("sparkSQL").getOrCreate()


empDF=spark.read.json("D:/movies/SparkPythonDoBigDataAnalytics-Resources/customerData.json")
empDF.show()

"""empDF.printSchema()

empDF.select("name").show()

empDF.filter(empDF["age"]==40).show()

empDF.groupBy("gender").count().show()

empDF.groupBy("deptid"). \
    agg({"salary":"avg","age":"max"}).show() """
    
    
deptList=[{'name':'sales','id':"100"}, \
          
          {'name':'engineering','id':"200"} ]


    #creating dataframe with list
    
deptDF=spark.createDataFrame(deptList)
deptDF.show()
    