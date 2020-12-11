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
		.master("local[*]")\ #Instanciation
		.appName("wordCount")\
		.getOrCreate() # Vérifer si une séssion est ouverte et l'ouvrir sinon
		
	
# Importation des données
lines = sc.textFile("sample.txt")

# Decoupage du texte par mots 
words = lines.flatMap(lambda line: line.split(" "))
    
# Compter le nombre d'occurence pour chaque mot:
tuples = words.map(lambda word: (word, 1))
	
# Regroupement des mots :
count = tuples.reduceByKey(lambda a,b:a +b)

counts = count.collect() # Execution des fonctions au dessus


# Affichage du resultat
print(counts)

for (word, count) in counts:
    print(word, count)

# Sauvegarde du résultat :
count.coalesce(1).saveAsTextFile("Result")

