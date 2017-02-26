from pyspark import SQLContext
from pyspark import SparkContext

sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/mohiulalamprince/work/python/cricstat/player-info-[0-9]*')
df.registerTempTable('players')
sqlContext.sql("select Player, sum(Runs) as total_runs from players group by Player order by total_runs desc").show()
