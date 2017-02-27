from pyspark import SQLContext
from pyspark import SparkContext

sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/mohiulalamprince/work/python/cricstat/player-info-[0-9]*')
df.registerTempTable('players')
sqlContext.sql("select count(*) from players").show()
