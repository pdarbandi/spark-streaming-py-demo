from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, col

spark = SparkSession.builder\
                    .appName("StructuredKafkaStream")\
                    .getOrCreate()

inputDF = spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers","spark-master:9092")\
                .option("subscribe","spark-kafka-demo")\
                .load()

strDF = inputDF.select(col("value").cast("String").alias("message"))

countsDF = strDF.withColumn("wordCount", size(split(col("message"), " ")))

query = countsDF.writeStream\
                .outputMode("update")\
                .format("console")\
                .start()

query.awaitTermination()

