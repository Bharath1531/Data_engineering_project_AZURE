# Databricks notebook source

dbutils.fs.ls('/mnt/bronze/SalesLT/')

# Transfromed data is needed to copy to the silver container in the Azure Data Lake Gen2

dbutils.fs.ls('mnt/silver/')

#Address table from the SalesLT is assinged to input_path

input_path='/mnt/bronze/SalesLT/Address/Address.csv'

# Read the data which is stored in the input_path

df=spark.read.format('csv').option("header","true").load(input_path)

# To display the Address table values which is stored as df

display(df)

# Import the required libraries 

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

#Add a new column as ModifiedDate to the DataFrame, which contains the formatted date values.

df =df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

# COMMAND ----------

display(df)

#Loop that iterates through a list of table names, reads CSV files for each table, and processes date columns in each DataFrame

for i in table_name:
    path ='/mnt/bronze/SalesLT/' + i + '/' + i + '.csv'
    df =spark.read.format('csv').load(path)
    column =df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"), 
                   "yyyy-MM-dd"))

#Load the transfromed data to the silver container in the Azure Data Lake Gen2

output_path='/mnt/silver/SalesLT/' +i +'/'
df.write.format('delta').mode("overwrite").save(output_path)

# Data transfromation for all the tables in the SalesLT

table_name=[]

for i in dbutils.fs.ls('/mnt/bronze/SalesLT/'):
     table_name.append(i.name.split('/')[0])

# Display al the table names

table_name

# Import the required libraries

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

#Iterate through the each tables which is stored in SalesLT

for i in table_name:
    path ='/mnt/bronze/SalesLT/' + i + '/' + i + '.csv'
    df =spark.read.format('csv').option('mergeSchema','true').option('header','true').load(path)
    column =df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"), "yyyy-MM-dd"))

   #Load the transformed data to the silver container in the Azure Data Lake Gen2
   
    output_path='/mnt/silver/SalesLT/' +i +'/'
    df.write.format('delta').mode("overwrite").option('overwriteSchema','true').save(output_path)

# Used to display the table with values

display(df)
