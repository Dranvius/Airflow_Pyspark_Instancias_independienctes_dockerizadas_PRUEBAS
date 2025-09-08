from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PiApp").getOrCreate()

# Estimación de Pi
def inside(p):
    from random import random
    x, y = random(), random()
    return x*x + y*y < 1

n = 100000
count = spark.sparkContext.parallelize(range(0, n)).filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()
