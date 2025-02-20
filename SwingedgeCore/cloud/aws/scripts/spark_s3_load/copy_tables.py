# read from source table
df4 = spark.read.format("jdbc")\
            .option("url",jdbc_url_src)\
            .option("user", u_src)\
            .option("password", p_src)\
            .option("dbtable",table_src)\
            .option("numPartitions", numPartitions)\
            .option("partitionColumn", "id")\
            .option("lowerBound", min_id)\
            .option("upperBound", max_id)\
            .option("driver","org.postgresql.Driver")\
            .load()
            
# write to target table
df4.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "us_etfs_stocks_1") \
    .option("user", u) \
    .option("password", p) \
    .option("driver", "org.postgresql.Driver") \
    .option("numPartitions", 40) \
    .option("partitionColumn", "id")\
    .option("lowerBound", min_id)\
    .option("upperBound", max_id)\
    .mode("overwrite").save()