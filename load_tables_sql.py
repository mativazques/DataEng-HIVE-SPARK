# Carga inicial del documento en hdfs a un dataframe llamado df

df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")
df.createOrReplaceTempView("vs_df")

# Carga de la tabla payments

df_payments = spark.sql("select cast(VendorID as int), cast(tpep_pickup_datetime as date), cast(payment_type as int),  cast(total_amount as float) from vs_df")

df_payments.createOrReplaceTempView("vs_payments_load")

spark.sql("insert into tripdb.payments select * from vs_payments_load")

# Carga de la tabla passenger

df_passengers = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int),  cast(total_amount as float) from vs_df")

df_passengers.createOrReplaceTempView("vs_passengers_load")

spark.sql("insert into tripdb.passenger select * from vs_passengers_load")

# Carga de la tabla tolls

df_tolls = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int), cast(tolls_amount as float),  cast(total_amount as float) from vs_df")

df_tolls.createOrReplaceTempView("vs_tolls_load")

spark.sql("insert into tripdb.tolls select * from vs_tolls_load")

# Carga de la tabla congestion
 
df_congestion = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int), cast(congestion_surcharge as float), cast(total_amount as float) from vs_df")

df_congestion.createOrReplaceTempView("vs_congestion_load")

spark.sql("insert into tripdb.congestion select * from vs_congestion_load")

# Carga de la tabla distance

df_distance = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int), cast(trip_distance as float), cast(total_amount as float) from vs_df")

df_distance.createOrReplaceTempView("vs_distance_load")

spark.sql("insert into tripdb.distance select* from vs_distance_load")
