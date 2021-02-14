import sys

from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
	conf = SparkConf().setAppName("Carrega Cidades").set("master","local")
	sc = SparkContext(conf = conf)
	cities_rdd = sc.textFile("../input/texto.txt")
	# collect the RDD to a list
	list_elements = cities_rdd.collect()
	# print the list
	for element in list_elements:
		print(element)  