import sys
import re


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega Cidades").set("master","local")
	sc = SparkContext(conf = conf)

	text = sc.textFile("../input/soma.txt")

	newEmpRDD = text.flatMap(lambda line : line.split("\n"))

	saida = newEmpRDD.map(lambda x:(x.split(","))).map(lambda line: ((line[0],line[1]),int(line[3]))).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0][0],x[0][1],x[1]))
	saida = saida.collect()
	for element in saida:
		print(element)
