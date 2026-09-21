from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DecimalType, TimestampType)
import time


spark = (SparkSession.builder
         .appName("modulo2-carregamento")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

# 1. Inferência
t0 = time.time()
df_inf = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("dados/pix_csv"))
print(f"inferSchema: {time.time() - t0:.2f}s")
df_inf.printSchema()

# 2. Schema explícito
schema = StructType([
    StructField("end_to_end_id",   StringType(),      False),
    StructField("conta_pagador",   StringType(),      False),
    StructField("conta_recebedor", StringType(),      False),
    StructField("valor",           DecimalType(12,2), False),
    StructField("data_hora",       TimestampType(),   False),
    StructField("status",          StringType(),      False),
    StructField("tipo_chave",      StringType(),      True),
])

t0 = time.time()
df = (spark.read
      .option("header", "true")
      .schema(schema)
      .csv("dados/pix_csv"))
print(f"schema explícito: {time.time() - t0:.2f}s")
df.printSchema()

spark.sparkContext.setJobDescription("leitura arquivo")
print("inferencia", df_inf.count())
print("explicita", df.count())

print("inferencia", df_inf.agg(F.sum("valor")).show(truncate=False))
print("explicita", df.agg(F.sum("valor")).show(truncate=False))

input("Abra http://localhost:4040 e pressione Enter para encerrar...")
spark.stop()