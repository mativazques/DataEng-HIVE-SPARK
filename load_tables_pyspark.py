df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")

df.show(1)

df_payments = df.select(df.VendorID.cast("int"), df.tpep_pickup_datetime.cast("date"), df.payment_type.cast("int"), df.total_amount.cast("float"))

df_payments.write.insertInto("tripdb.payments")
