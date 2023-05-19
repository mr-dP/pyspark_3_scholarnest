# Databricks notebook source
diamonds_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)

diamonds_df.show(10)

# COMMAND ----------

from pyspark.sql.functions import avg, round

results_df = (
    diamonds_df.select("color", "price")
    .groupBy("color")
    .agg(round(avg("price"), 2).alias("avg_price"))
)

results_df.show()

# COMMAND ----------

display(results_df)

# COMMAND ----------


