# Databricks notebook source
dbutils.fs.ls('/mnt/bronze/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

input_path='/mnt/bronze/SalesLT/Address/Address.csv'

# COMMAND ----------

df=spark.read.format('csv').option("header","true").load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

df =df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

# COMMAND ----------

display(df)

# COMMAND ----------

for i in table_name:
    path ='/mnt/bronze/SalesLT/' + i + '/' + i + '.csv'
    df =spark.read.format('csv').load(path)
    column =df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"), 
                   "yyyy-MM-dd"))


output_path='/mnt/silver/SalesLT/' +i +'/'
df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('/mnt/bronze/SalesLT/'):
     table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path ='/mnt/bronze/SalesLT/' + i + '/' + i + '.csv'
    df =spark.read.format('csv').option('mergeSchema','true').option('header','true').load(path)
    column =df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"), "yyyy-MM-dd"))

    output_path='/mnt/silver/SalesLT/' +i +'/'
    df.write.format('delta').mode("overwrite").option('overwriteSchema','true').save(output_path)

# COMMAND ----------

display(df)