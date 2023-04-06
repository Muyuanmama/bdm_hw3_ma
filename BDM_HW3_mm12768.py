#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def round_percentage(value):
    return int(round(value))

if __name__ == "__main__":


    input_path, output_path = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("BDM_HW3").getOrCreate()


    complaints_df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"'
    )

    complaints_df = complaints_df.withColumn("Product", F.lower(F.col("Product"))) \
                              .withColumn("Company", F.lower(F.col("Company"))) \
                              .withColumn("Year", F.year(F.col("Date received")))

    complaints_grouped = complaints_df.groupBy("Product", "Year", "Company").count()

    summary = complaints_grouped.groupBy("Product", "Year") \
                            .agg(F.sum("count").alias("total_complaints"),
                                 F.count("Company").alias("num_companies"))

    max_complaints = complaints_grouped.groupBy("Product", "Year") \
                                   .agg(F.max("count").alias("max_complaints"))

    result = summary.join(max_complaints, ["Product", "Year"])

    result = result.withColumn("highest_percentage",
                           F.round((F.col("max_complaints") / F.col("total_complaints")) * 100))

    result = result.select("Product", "Year", "total_complaints", "num_companies", "highest_percentage") \
               .orderBy("Product", "Year")

    formatted_result = result.withColumn(
        "output",
        F.concat_ws(
            ",",
            F.col("Product"),
            F.col("Year"),
            F.col("total_complaints"),
            F.col("num_companies"),
            F.col("highest_percentage"),
        ),
    ).select("output")
    
    results_df.write.csv(
        output_path,
        mode="overwrite",
        header=False,
        emptyValue=""
    )

    spark.stop()

