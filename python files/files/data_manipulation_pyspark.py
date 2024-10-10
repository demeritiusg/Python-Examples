from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('FlightAnalysis').getOrCreate()

flight_data_path = ''

df = spark.read.cvs(flight_data_path, header=True, inferSchema=True)

df_clean = df.dropna()

df_clean = df_clean.withColumnRenamed("ORIGIN_AIRPORT", "Origin") \
				   .withColumnRenamed("DESTINATION_AIRPORT", "Destination") \
				   .withColumnRenamed("DEPARTURE_DELAY", "DepDelay") \
				   .withColumnRenamed("ARRIVAL_DELAY", "ArrDelay")


df_clean = df_clean.withColumn('TotalDelay', df_clean['DepDelay'] +  df_clean['ArrDelay'])

df_avg_delay = df_clean.groupby('AIRLINE').agg({'TotalDelay':'avg'})
df_avg_delay = df_avg_delay.withColumnRenamed('avg(TotalDelay)', 'AvgDelay')

df_most_delays = df_clean.filter(df_clean['TotalDelay'] > 0).orderBy('TotalDelay', ascending=False)
df_avg_delay_sorted = df_avg_delay.orderBy('AvgDelay', ascending=False)

df_most_delays.show(5)
df_avg_delay_sorted.show(5)