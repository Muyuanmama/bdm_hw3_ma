#!/usr/bin/env python
# coding: utf-8

# In[ ]:


gdown --quiet 1-IeoZDwT5wQzBUpsaS5B6vTaP-2ZBkam
pip --quiet install pyspark

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# Create a Spark session
spark = SparkSession.builder.getOrCreate()


# In[ ]:


complaints_df = spark.read.csv("complaints.csv", header=True)

complaints_df = complaints_df.withColumn("Product", F.lower(F.col("Product"))) \
                              .withColumn("Company", F.lower(F.col("Company"))) \
                              .withColumn("Year", F.year(F.col("Date received")))

# Group by product, year, and company, and count the number of complaints per group
complaints_grouped = complaints_df.groupBy("Product", "Year", "Company").count()

# Compute the total number of complaints and the number of companies for each product and year
summary = complaints_grouped.groupBy("Product", "Year") \
                            .agg(F.sum("count").alias("total_complaints"),
                                 F.count("Company").alias("num_companies"))

# Find the company with the highest number of complaints for each product and year
max_complaints = complaints_grouped.groupBy("Product", "Year") \
                                   .agg(F.max("count").alias("max_complaints"))

# Join the summary and max_complaints DataFrames
result = summary.join(max_complaints, ["Product", "Year"])

# Calculate the highest percentage of complaints for a single company
result = result.withColumn("highest_percentage",
                           F.round((F.col("max_complaints") / F.col("total_complaints")) * 100))

# Select the desired output columns and sort the result
result = result.select("Product", "Year", "total_complaints", "num_companies", "highest_percentage") \
               .orderBy("Product", "Year")

# Format output rows as strings
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

