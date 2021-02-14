import sys
import re


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega Texto").set("master","local")
	sc = SparkContext(conf = conf)
	#'city', 'city_ibge_code', 'date', 'epidemiological_week', 'estimated_population',
	# 'estimated_population_2019', 'is_last', 'is_repeated', 'last_available_confirmed', 'last_available_confirmed_per_100k_inhabitants',
	# 'last_available_date', 'last_available_death_rate', 'last_available_deaths', 'order_for_place', 'place_type', 'state', 'new_confirmed', 'new_deaths'

	data = sc.binaryFiles("../input/caso_full.csv").values().flatMap(lambda x: x.decode("UTF-8").split("\n"))
	data = data.filter(lambda x: x.split(',')[0] not in 'city')
	data = data.map(lambda x: x.split(',')).map(lambda line: ((line[2],line[15]), int(line[17]))).reduceByKey(lambda a,b : a + b).sortByKey()

	data.saveAsTextFile("../output/sum1")
