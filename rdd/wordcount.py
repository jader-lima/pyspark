import sys
import re

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Carrega Texto").set("master","local")
	sc = SparkContext(conf = conf)
	text = sc.textFile("../input/palavras.txt")
	stopwords = ['o', 'a', 'de', 'da', 'que', 'os', 'as','em','um','umas']

	words = text.flatMap(lambda line : line.split(" "))

	words = words.map(lambda word : re.sub(r'[^a-zA-Z0-9\s]','',word))

	words = words.map(lambda line: line.lower())

	resultwords  = words.filter(lambda word : word  not in stopwords)

	#conta a ocorrencia de palavras
	count = resultwords.map(lambda palavra : (palavra,1)).reduceByKey(lambda a,b : a + b).sortByKey()

	#a, b onde a chave, b soma da contagem
	#a + b - para cada chave, a + b seria 1 + 1 de uma mesma chave

	#salvar resultado
	count.saveAsTextFile("../output/wordcount")

	