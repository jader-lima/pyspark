import sys

from pyspark.sql import SparkSession
if __name__ == "__main__":
	spark = SparkSession.builder.appName("PySpark create using parallelize() function RDD example").config("spark.some.config.option", "some-value").getOrCreate()  
	df = spark.sparkContext.parallelize([(12, 20, 35, 'a b c'), (41, 58, 64, 'd e f'), (70, 85, 90, 'g h i')]).toDF(['col1', 'col2', 'col3','col4'])  
	df.show()  