import sys
import re

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Carrega Texto").set("master", "local")
    sc = SparkContext(conf=conf)
    data = sc.binaryFiles("../input/caso_full.csv").values().flatMap(lambda x: x.decode("UTF-8").split("\n"))
    data = data.filter(lambda x: x.split(',')[0] not in 'city').filter(lambda x: x.split(',')[0] not in '' )
    data = data.map(lambda x: x.split(',')).map(lambda line: ((line[2], line[15]), int(line[17]))).reduceByKey(lambda a, b: a + b).sortByKey()

    data.saveAsTextFile("../output/sum2")
