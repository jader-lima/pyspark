import sys
import re


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega todos txt com mesmo padrão de nome").set("master","local")
	sc = SparkContext(conf = conf)
	text = sc.textFile("../input/texto*.txt")

	data = text.map(lambda line : line.split("\n"))
	#pyspark.rdd.RDD retorna um pipelin RDD, não é possivel fazer o split diretamente, pois cada elemento é uma lista , por isso o x[0].split
	#cada elemento fica dentro de uma lista de um elemento

	saida = data.map(lambda x:(x[0].split(" ")) ).map(lambda line: ((line[0]),int(line[1]))).reduceByKey(lambda x,y:x+y)
	
	count = saida.collect()
	for element in count:
		print(element)
























