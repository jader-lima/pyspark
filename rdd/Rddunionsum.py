import sys
import re


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega Cidades").set("master","local")
	sc = SparkContext(conf = conf)
	text1 = sc.textFile("../input/texto1.txt")
	text2 = sc.textFile("../input/texto2.txt")
	

	data1 = text1.map(lambda line : line.split("\n"))

	data2 = text2.map(lambda line : line.split("\n"))

	union = sc.union([text1, text2])
	

	saida = union.map(lambda x:(x.split(" "))).map(lambda line: ((line[0]),int(line[1]))).reduceByKey(lambda x,y:x+y)
	
	count = saida.collect()
	for element in count:
		print(element)
























