from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (StructType, StructField, StringType,
                               DecimalType, TimestampType)
import time


spark = (SparkSession.builder
         .appName("modulo2-carregamento")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())


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

## agregador
volume_diario = (df
                 .filter(F.col("status") == "CONFIRMADO")
                 .withColumn("data", F.to_date(F.col("data_hora")))
                 .groupBy("conta_pagador", "data")
                 .agg(F.count("*").alias("qtd"),
                      F.sum(F.col("valor")).alias("total")))

volume_diario.orderBy(F.desc("total")).show(10)
print(volume_diario)

print("===========================")
## window
por_recebedor = (
    df.filter(F.col("status") == "CONFIRMADO")
    .groupBy("conta_recebedor")
    .agg(F.sum("valor").alias("total_recebido"),
         F.count("*").alias("qtd"))
)

w_rank = Window.orderBy(F.desc("total_recebido"))

top = (por_recebedor
       .withColumn("posicao", F.rank().over(w_rank))
       .filter(F.col("posicao") <= 20))

top.show(20)
print(top)

print("===========================")
w_conta = Window.partitionBy("conta_pagador").orderBy("data_hora")

intervalos = (df
    .withColumn("anterior", F.lag("data_hora").over(w_conta))
    .withColumn("segundos_desde_anterior",
                F.unix_timestamp("data_hora") - F.unix_timestamp("anterior"))
    .withColumn("acumulado_dia",
                F.sum("valor").over(
                    Window.partitionBy("conta_pagador", F.to_date("data_hora"))
                          .orderBy("data_hora")))
    )

# Suspeitas: mais de uma transação em menos de 60 segundos
(interva
 .filter(F.col("segundos_desde_anterior") < 60)
 .select("conta_pagador", "data_hora", "valor",
         "segundos_desde_anterior", "acumulado_dia")
 .orderBy("conta_pagador", "data_hora")
 .show(20, truncate=False))


intervalos.explain("formatted")

print("=======================")


bloqueadas = spark.createDataFrame(
    [("000000000042",), ("000000000777",), ("000000001234",)],
    ["conta"])

# Transações de contas bloqueadas
suspeitas = df.join(bloqueadas, df.conta_pagador == bloqueadas.conta, "inner")

# Transações de contas NÃO bloqueadas
limpas = df.join(bloqueadas, df.conta_pagador == bloqueadas.conta, "left_anti")

print("suspeitas:", suspeitas.count(), "| limpas:", limpas.count())

input("Abra http://localhost:4040 e pressione Enter para encerrar...")
spark.stop()