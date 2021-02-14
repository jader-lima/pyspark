
import sys
import re


from pyspark.sql import SparkSession

if __name__ == "__main__":
#ID| TIPO| NM_BAIRRO| NM_DISTRITO|CD_GEOCODMU| NM_MUNICIPIO|        
# NM_MICRO| NM_MESO| NM_UF|CD_NIVEL|CD_CATEGORIA|NM_CATEGORIA| NM_LOCALIDADE| LONG| LAT| ALT|

	spark = SparkSession.builder.appName('chosenName').getOrCreate()
	#df = spark.read.csv("../caso_full/caso_full.csv", header=True, inferSchema=True)
	df = spark.read.csv("../input/BR_mun_2021.csv", header=True, inferSchema=True, encoding="ISO-8859-1")
	
	df.show(n=30)

	l = ['CIDADE','POVOADO']
	df.filter(df.NM_CATEGORIA.isin(l)).show(n=30)