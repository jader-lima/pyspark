import sys
import re


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega Texto").set("master","local")
	sc = SparkContext(conf = conf)

	text = sc.textFile("../input/BR_mun_2021.csv")

	data = text.map(lambda line : line.split("\n"))
	saida = data.map(lambda x:(x[0].split(",")) ).map(lambda line: ((line[1],line[3],line[8],line[11]), 1)).reduceByKey(lambda a,b : a + b).sortByKey()
	saida.saveAsTextFile("../output/count1")

