from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/mohiulalamprince/work/python/cricstat/player-info-[0-9]*')

def formatting_unbitten_and_dnb(run):
    if run.endswith('*'):
        return run[:-1]
    elif (run == 'TDNB' or run == 'DNB'):
        return 0
    return run

ignore_dnb = udf(formatting_unbitten_and_dnb, StringType())
df = df.withColumn("Runs", ignore_dnb("Runs"))
df.registerTempTable('players')
sqlContext.sql("select Player, sum(Runs) as total_runs from players group by Player order by total_runs desc").show()
