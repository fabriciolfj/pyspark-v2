from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("modulo2-schemas")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

tx = (spark.range(0, 1_000_000, numPartitions=8)
      .select(
          F.concat(F.lit("E"), F.lpad(F.col("id").cast("string"), 12, "0")).alias("end_to_end_id"),
          F.concat(F.lit("0000"), F.lpad((F.col("id") % 5000).cast("string"), 8, "0")).alias("conta_pagador"),
          F.concat(F.lit("0000"), F.lpad((F.rand(seed=7) * 3000).cast("int").cast("string"), 8, "0")).alias("conta_recebedor"),
          (F.rand(seed=42) * 5000).cast("decimal(12,2)").alias("valor"),
          F.from_unixtime(F.lit(1735689600) + (F.rand(seed=3) * 2592000).cast("int")).alias("data_hora"),
          F.when(F.col("id") % 10 == 0, "ESTORNADO").otherwise("CONFIRMADO").alias("status"),
          F.when(F.col("id") % 3 == 0, "CPF")
           .when(F.col("id") % 3 == 1, "EMAIL")
           .otherwise("ALEATORIA").alias("tipo_chave"))
      )

tx.write.mode("overwrite").option("header", "true").csv("dados/pix_csv")