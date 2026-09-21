from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("modulo1-fundamentos")
         .master("local[2]")                              # 2 threads = 2 "executores"
         .config("spark.sql.shuffle.partitions", "2")    # padrão é 200, exagero local
.config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

# Transformações: nada executa ainda
tx = (spark.range(0, 10_000_000, numPartitions=4)
      .withColumn("valor", (F.rand(seed=42) * 1000).cast("decimal(12,2)"))
      .withColumn("status",
                  F.when(F.col("id") % 10 == 0, "ESTORNADO")
                   .otherwise("CONFIRMADO")))

acima_500 = tx.filter(F.col("valor") > 500)     # narrow
por_status = (tx.groupBy("status")                # wide -> shuffle
                .agg(F.count("*").alias("qtd"),
                     F.sum("valor").alias("total")))

spark.sparkContext.setJobDescription("count acima de 500")
# Ações: aqui os jobs nascem
print("Acima de 500:", acima_500.count())

spark.sparkContext.setJobDescription("group by status")
por_status.show()

por_status.explain("formatted")

input("Abra http://localhost:4040 e pressione Enter para encerrar...")
spark.stop()