# Databricks notebook source
# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 3
# MAGIC
# MAGIC This exercise has three parts:
# MAGIC
# MAGIC - tasks 1-3 concern data queries for static data
# MAGIC - tasks 4-5 are examples of using typed Dataset instead of DataFrame
# MAGIC - tasks 6-8 concern the same data query as in the first two tasks but handled as streaming data
# MAGIC
# MAGIC The tasks can be done in either Scala or Python. This is the **Python** version, switch to the Scala version if you want to do the tasks in Scala.
# MAGIC
# MAGIC Each task has its own cell for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with example outputs or test code following most of the tasks.
# MAGIC
# MAGIC Don't forget to submit your solutions to Moodle.
# MAGIC

# COMMAND ----------

# some imports that might be required in the tasks

from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions
from pyspark.sql import Row


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Create a DataFrame for retailer data
# MAGIC
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in the `exercises/ex3` folder is file `sales_data_sample.csv` that contains retaier sales data in CSV format.
# MAGIC The direct address for the data file is: `abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex3/sales_data_sample.csv`
# MAGIC
# MAGIC Read the data from the CSV file into DataFrame called retailerDataFrame. Let Spark infer the schema for the data. Note, that this CSV file uses semicolons (`;`) as the column separator instead of the default commas (`,`).
# MAGIC
# MAGIC Print out the schema, the resulting DataFrame should have 24 columns. The data contains information about the item price and the number of items ordered for each day.
# MAGIC

# COMMAND ----------


file = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex3/sales_data_sample.csv"
retailerDataFrame: DataFrame  = spark.read.option("sep", ";").option("header", "true").option("inferSchema", "true").csv(file)

#display(DataFrame)
retailerDataFrame.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 1 (only the first few lines):
# MAGIC
# MAGIC ```text
# MAGIC root
# MAGIC  |-- ORDERNUMBER: integer (nullable = true)
# MAGIC  |-- QUANTITYORDERED: integer (nullable = true)
# MAGIC  |-- PRICEEACH: double (nullable = true)
# MAGIC  |-- ORDERLINENUMBER: integer (nullable = true)
# MAGIC  |-- ORDERDATE: date (nullable = true)
# MAGIC  ...
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - The best selling days
# MAGIC
# MAGIC Find the best **12** selling days using the retailer data frame from task 1. That is the days for which `QUANTITYORDERED * PRICEEACH` gets the highest values.
# MAGIC

# COMMAND ----------

best12DaysDF: DataFrame = retailerDataFrame.withColumn("total_sales", functions.col("QUANTITYORDERED") * functions.col("PRICEEACH"))

best12DaysDF = best12DaysDF.groupBy("ORDERDATE").sum("total_sales")
best12DaysDF = best12DaysDF.orderBy(functions.col("sum(total_sales)").desc()).limit(12)

best12DaysDF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 2:
# MAGIC
# MAGIC ```text
# MAGIC +----------+------------------+
# MAGIC | ORDERDATE|       total_sales|
# MAGIC +----------+------------------+
# MAGIC |2004-11-24|115033.48000000003|
# MAGIC |2003-11-14|109509.87999999999|
# MAGIC |2003-11-12|          90218.58|
# MAGIC |2003-12-02| 87445.18000000001|
# MAGIC |2004-10-16|          86320.39|
# MAGIC |2003-11-06|          84731.32|
# MAGIC |2004-11-17|          82125.62|
# MAGIC |2004-11-04|          80807.93|
# MAGIC |2004-08-20| 80247.84000000001|
# MAGIC |2004-11-05| 78324.73000000001|
# MAGIC |2003-11-20|          76973.93|
# MAGIC |2004-12-10|          76380.08|
# MAGIC +----------+------------------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - The products with the most sale value
# MAGIC
# MAGIC Find the product codes for the **8** products that have the most total sale value in year 2003.
# MAGIC
# MAGIC **Note**, in this task (and only in this task) all sales done in **January** should be counted **twice**.
# MAGIC
# MAGIC Hint: use the MONTH_ID and YEAR_ID columns to recognize the month and year of each sale.
# MAGIC

# COMMAND ----------

productDF: DataFrame = ???

productDF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 3:
# MAGIC
# MAGIC ```text
# MAGIC +-----------+------------------+
# MAGIC |PRODUCTCODE|       total_sales|
# MAGIC +-----------+------------------+
# MAGIC |   S18_3232|           69500.0|
# MAGIC |   S18_2319|           45600.0|
# MAGIC |   S18_4600|           44400.0|
# MAGIC |   S50_1392|39813.920000000006|
# MAGIC |   S18_1342|39661.149999999994|
# MAGIC |   S12_4473|          39084.36|
# MAGIC |   S24_3856|           38900.0|
# MAGIC |   S24_2300|           38800.0|
# MAGIC +-----------+------------------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Dataset 1
# MAGIC
# MAGIC This task is originally designed for Scala language. It does not fully translate to Python because Python does not support typed Spark Datasets. Instead of Scala's case class, you can use Python's dataclass.
# MAGIC
# MAGIC The classes that takes a type parameter are known to be Generic classes in Scala. Dataset is an example of a generic class. Actually, DataFrame is a type alias for Dataset[Row], where Row is the type parameter (Row being general object that can represent any row in a Spark data frame).
# MAGIC
# MAGIC Declare your own case class (dataclass in Python) Sales with two members: `year` and `euros`, with both being of integer types.
# MAGIC
# MAGIC Then instantiate a DataFrame of Sales with data from the variable `salesList` and query for the sales on 2017 and for the year with the highest amount of sales.
# MAGIC

# COMMAND ----------

# Create the Sales dataclass in this cell

@dataclass
class Sales:
  year: int
  euros: int




# COMMAND ----------

salesList = [Sales(2015, 325), Sales(2016, 100), Sales(2017, 15), Sales(2018, 900),
             Sales(2019, 50), Sales(2020, 750), Sales(2021, 950), Sales(2022, 400)]

salesDS: DataFrame = spark.createDataFrame(salesList)

sales2017Row: Row = salesDS.filter(salesDS.year==2017).collect()
sales2017: Sales = sales2017Row[0]
print(f"Sales for 2017 is {sales2017.euros}")

#maximumSalesRow: Row = ???
#maximumSales: Sales = ???
#print(f"Maximum sales: year = {maximumSales.year}, euros = {maximumSales.euros}")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Example output for task 4:
# MAGIC ```text
# MAGIC Sales for 2017 is 15
# MAGIC Maximum sales: year = 2021, euros = 950
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Dataset 2
# MAGIC
# MAGIC Continuation from task 4.
# MAGIC The new sales list `multiSalesList` contains sales information from multiple sources and thus can contain multiple values for each year. The total sales in euros for a year is the sum of all the individual values for that year.
# MAGIC
# MAGIC Query for the sales on 2016 and the year with the highest amount of sales in this case.
# MAGIC

# COMMAND ----------

multiSalesList: List[Sales] =  [
    Sales(2015, 325), Sales(2016, 100), Sales(2017, 15), Sales(2018, 900),
    Sales(2019, 50), Sales(2020, 750), Sales(2021, 950), Sales(2022, 400),
    Sales(2016, 250), Sales(2017, 600), Sales(2019, 75), Sales(2016, 5),
    Sales(2018, 127), Sales(2019, 200), Sales(2020, 225), Sales(2016, 350)
]

multiSalesDS: DataFrame = ???

multiSales2016: Sales = ???
print(f"Total sales for 2016 is {multiSales2016.euros}")

maximumMultiSales: Sales = ???
print(f"Maximum total sales: year = {maximumMultiSales.year}, euros = {maximumMultiSales.euros}")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 5:
# MAGIC
# MAGIC ```text
# MAGIC ...
# MAGIC Total sales for 2016 is 705
# MAGIC Maximum total sales: year = 2018, euros = 1027
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Streaming data from retail data
# MAGIC
# MAGIC Create a streaming data frame for similar retailer data as was used in tasks 1-3.
# MAGIC
# MAGIC In this exercise, streaming data is simulated by copying CSV files in 10 second intervals from a source folder to a target folder. The script for doing the file copying is given in task 8 and should not be run before the tasks 6 and 7 have been done.
# MAGIC
# MAGIC The target folder will be defined to be in the students storage as `ex3/YOUR_EX3_FOLDER` with the value for YOUR_EX3_FOLDER chosen by you.
# MAGIC
# MAGIC Hint: Spark cannot infer the schema of streaming data, so you have to give it explicitly. You can assume that the streaming data will have the same format as the static data used in tasks 1-3.
# MAGIC
# MAGIC Finally, note that you cannot really test this task before you have also done the tasks 7 and 8.
# MAGIC

# COMMAND ----------

# Put your own unique folder name to the variable (use only letters, numbers, and underscores):
my_ex3_folder: str = ???

targetFiles: str = f"abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/{my_ex3_folder}/*"

retailerStreamingDF: DataFrame = ???


# COMMAND ----------

# MAGIC %md
# MAGIC Note that you cannot really test this task before you have also done the tasks 7 and 8, i.e. there is no checkable output from this task.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - The best selling days for the streaming data
# MAGIC
# MAGIC Find the best selling days using the streaming data frame from task 6.
# MAGIC
# MAGIC Note that in this task with the streaming data you don't need to limit the content only to the best 12 selling days like was done in task 2.
# MAGIC

# COMMAND ----------

bestDaysDFStreaming: DataFrame = ???


# COMMAND ----------

# MAGIC %md
# MAGIC Note that you cannot really test this task before you have also done the tasks 6 and 8, i.e. there is no checkable output from this task.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Test your streaming data solution
# MAGIC
# MAGIC Test your streaming data solution by following the output from the display command in the next cell while the provided test script in the following cell is running.
# MAGIC
# MAGIC - The test script copies files one by one (in ten second intervals) from the [shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) folder `exercises/ex3` to the [student container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/students/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) folder `/ex3/MY_EX3_FOLDER` where `MY_EX3_FOLDER` is the folder name you chose in task 6.
# MAGIC - To properly run the streaming data test, the target folder should be either empty or it should not exist at all. If there are files in the target folder, those are read immediately and the streaming data demostration will not work as intended. The script does try to remove all copied files from the target folder at the end, but that only happens if the script is not interrupted.
# MAGIC
# MAGIC To gain points from this task, answer the questions in the final cell of the notebook.
# MAGIC

# COMMAND ----------

# in Databricks the display function can be used to display also a streaming data frame
# when developing outside this kind of environment, you need to create a query that could then be used to monitor the state of the data frame
# Usually, when using streaming data the results are directed to some data storage, not just displayed like in this exercise.

# There should be no need to edit anything in this cell!
print(f"Starting stream myQuery_{my_ex3_folder}")
display(bestDaysDFStreaming.limit(6), streamName=f"myQuery_{my_ex3_folder}")


# COMMAND ----------

# There should be no need to edit anything in this cell, but you can try to adjust to time variables.

import glob
import pathlib
import shutil
import time

initial_wait_time: float = 20.0
time_interval: float = 10.0
post_loop_wait_time: float = 20.0

time.sleep(initial_wait_time)
input_file_list: list = dbutils.fs.ls("abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex3/streamingData")
for index, csv_file in enumerate(input_file_list, start=1):
    input_file_path = csv_file.path
    input_file = pathlib.Path(input_file_path).name
    output_file_path = f"abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/{my_ex3_folder}/{input_file}"
    dbutils.fs.cp(input_file_path, output_file_path)
    print(f"Copied file {input_file} ({index}/{len(input_file_list)}) to {output_file_path}")
    time.sleep(time_interval)
time.sleep(post_loop_wait_time)

# stop updating the display for the streaming data frame
for stream in spark.streams.active:
    if stream.name == f"myQuery_{my_ex3_folder}":
        print(f"Stopping stream {stream.name}")
        spark.streams.get(stream.id).stop()

# remove the copied files from the output folder
for copy_file in dbutils.fs.ls(f"abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/{my_ex3_folder}"):
    dbutils.fs.rm(copy_file.path)
print(f"Removed all copied files from abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/{my_ex3_folder}")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output from the test script in task 8:
# MAGIC
# MAGIC ```text
# MAGIC Copied file xaa.csv (1/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xaa.csv
# MAGIC Copied file xab.csv (2/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xab.csv
# MAGIC Copied file xac.csv (3/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xac.csv
# MAGIC Copied file xad.csv (4/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xad.csv
# MAGIC Copied file xae.csv (5/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xae.csv
# MAGIC Copied file xaf.csv (6/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xaf.csv
# MAGIC Copied file xag.csv (7/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xag.csv
# MAGIC Copied file xah.csv (8/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xah.csv
# MAGIC Copied file xai.csv (9/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xai.csv
# MAGIC Copied file xaj.csv (10/10) to abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here/xaj.csv
# MAGIC Stopping stream myQuery_some_folder_here
# MAGIC Removed all copied files from abfss://students@tunics320f2023gen2.dfs.core.windows.net/ex3/some_folder_here
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer the questions to get the points from task 8.**
# MAGIC
# MAGIC How well did the streaming data example work for you?
# MAGIC
# MAGIC What was the final output for the streaming data for you?
# MAGIC
# MAGIC The data in the streaming tasks is the same as the earlier static data (just divided into multiple files).
# MAGIC Did the final output match the first six rows of the task 2 output?
# MAGIC
# MAGIC If it did not, what do you think could be the reason?
# MAGIC
# MAGIC If you had problems, what were they?
# MAGIC And what do you think were the causes for those problems?
# MAGIC
