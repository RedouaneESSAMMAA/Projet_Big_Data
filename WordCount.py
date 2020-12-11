# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 00:00:00 2020

@author: RedOne
"""

import findspark

findspark.init("C:\Spark")

from pyspark import SparkContext
from pyspark.sql import SparkSession


   
spark = SparkSession.builder\
		.master("local[*]")\
		.appName("wordCount")\
		.getOrCreate() # Vérifer si une séssion est ouverte et l'ouvrir sinon
		
	
# read data from text file and split each line into words
lines = sc.textFile("C:/Users/RedOne/Desktop/WordCount/sample.txt")
    
words = lines.flatMap(lambda line: line.split(" "))
    
# count the occurrence of each word
tuples = words.map(lambda word: (word, 1))
	
count = tuples.reduceByKey(lambda a,b:a +b)

# save the counts to output
count.coalesce(1).saveAsTextFile("C:/Users/RedOne/Desktop/WordCount/Result")

