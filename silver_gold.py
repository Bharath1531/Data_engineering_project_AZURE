# Databricks notebook source
dbutils.fs.ls('/mnt/silver/SalesLT/')

# Transformed data is needed to copy to the gold container in the Azure Data Lale Gen2

dbutils.fs.ls('/mnt/gold')

# Address table is stored in input_path

input_path='/mnt/silver/SalesLT/Address/'

# Read the data which is stored in the input_path

df= spark.read.format('delta').load(input_path)

# Display the Address table values

display(df)

# Import the required libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_replace

column_names=df.columns

for old_col_name in column_names:
    new_col_name ="".join(["_"+char if char.isupper() and not old_col_name[i -1].isupper() else char for i,char in enumerate(old_col_name)]).lstrip("_")

    df =df.withColumnRenamed(old_col_name,new_col_name)

# COMMAND ----------

display(df)

# Data Transformation for all the tables in SalesLT

table_name=[]

for i in dbutils.fs.ls('/mnt/silver/SalesLT/'):
     table_name.append(i.name.split('/')[0])

# Diaplay the table names

table_name

# Data transformed as required value

for name in table_name:
    path='/mnt/silver/SalesLT/' + name
    print(path)
    df = spark.read.format('delta').load(path)

    column_names =df.columns

    for old_col_name in column_names:
        new_col_name ="".join(["_"+char if char.isupper() and not old_col_name[i -1].isupper() else char for i,char in enumerate(old_col_name)]).lstrip("_")

        df =df.withColumnRenamed(old_col_name,new_col_name)

#Load the transformed data into gold container in Azure Data Lake Gen2

    output_path ='/mnt/gold/SaesLT/' + name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)


# Display the table with values

display(df)
