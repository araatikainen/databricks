# Databricks notebook source
# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 1
# MAGIC
# MAGIC This exercise is mostly introduction to the Azure Databricks notebook system.
# MAGIC These are some basic programming tasks that can be done in either Scala or Python. This is the **Python** version, switch to the Scala version if you want to do the task in Scala.
# MAGIC
# MAGIC Each task has its own cell for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with test code following most of the tasks that involve producing code.
# MAGIC
# MAGIC Don't forget to submit your solutions to Moodle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read tutorial
# MAGIC
# MAGIC Read the "[Basics of using Databricks notebooks](https://adb-5736551434993186.6.azuredatabricks.net/?o=5736551434993186#notebook/1892052735998707/command/1892052735998713)" tutorial notebook, at least the initial information and the first code examples. Clone the tutorial notebook to your own workspace and run at least those first code examples.
# MAGIC
# MAGIC To get a point from this task, add "done" (or something similar) to the following cell (after you have read the tutorial).

# COMMAND ----------

# MAGIC %md
# MAGIC Task 1 is done

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Basic function
# MAGIC
# MAGIC In the following cell write a simple function `mySum`, that takes two integer as parameters and returns their sum.

# COMMAND ----------

def mySum(i, j):
    return i+j

# COMMAND ----------

# you can test your function by running both the previous and this cell

sum41 = mySum(20, 21)
if sum41 == 41:
    print(f"correct result: 20+21 = {sum41}")
else:
    print(f"wrong result: {sum41} != 41")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Fibonacci numbers
# MAGIC
# MAGIC The Fibonacci numbers, `F_n`, are defined such that each number is the sum of the two preceding numbers. The first two Fibonacci numbers are:
# MAGIC
# MAGIC $$F_0 = 0 \qquad F_1 = 1$$
# MAGIC
# MAGIC In the following cell, write a **recursive** function, `fibonacci`, that takes in the index and returns the Fibonacci number. (no need for any optimized solution here)
# MAGIC

# COMMAND ----------

def fibonacci(index):
    
    if index <= 0:
        return 0
    
    elif index == 1:
        return 1
    
    else:
        return fibonacci(index-1) + fibonacci(index-2)

# COMMAND ----------

fibo6 = fibonacci(6)
if fibo6 == 8:
    print("correct result: fibonacci(6) == 8")
else:
    print(f"wrong result: {fibo6} != 8")

fibo11 = fibonacci(11)
if fibo11 == 89:
    print("correct result: fibonacci(11) == 89")
else:
    print(f"wrong result: {fibo11} != 89")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Higher order functions 1
# MAGIC
# MAGIC Use functions `map` and `reduce` to compute the sum of cubes of the values in the given list.

# COMMAND ----------

from functools import reduce
from operator import add
myList = [2, 3, 5, 7, 11, 13, 17, 19]
listCube = map(lambda x: x**3, myList)
cubeSum = reduce(add, list(listCube))
print(cubeSum)

# COMMAND ----------

if cubeSum == 15803:
    print(f"correct result: {cubeSum} == 15803")
else:
    print(f"wrong result: {cubeSum} != 15803")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Higher order functions 2
# MAGIC
# MAGIC Explain the following Scala code snippet (Python versions given at the end). You can try the snippet piece by piece in a notebook cell or search help from Scaladoc ([https://www.scala-lang.org/api/2.12.x/](https://www.scala-lang.org/api/2.12.x/)).
# MAGIC
# MAGIC ```scala
# MAGIC "sheena is a punk rocker she is a punk punk"
# MAGIC     .split(" ")
# MAGIC     .map(s => (s, 1))
# MAGIC     .groupBy(p => p._1)
# MAGIC     .mapValues(v => v.length)
# MAGIC ```
# MAGIC
# MAGIC What about?
# MAGIC
# MAGIC ```scala
# MAGIC "sheena is a punk rocker she is a punk punk"
# MAGIC     .split(" ")
# MAGIC     .map((_, 1))
# MAGIC     .groupBy(_._1)
# MAGIC     .mapValues(v => v.map(_._2).reduce(_+_))
# MAGIC ```
# MAGIC
# MAGIC For those that don't want to learn anything about Scala, you can do the explanation using the following Python versions:
# MAGIC
# MAGIC ```python
# MAGIC from itertools import groupby  # itertools.groupby requires the list to be sorted
# MAGIC {
# MAGIC     r: len(s) 
# MAGIC     for r, s in {
# MAGIC         p: list(v) 
# MAGIC         for p, v in groupby(
# MAGIC             sorted(
# MAGIC                 list(map(
# MAGIC                     lambda x: (x, 1),
# MAGIC                     "sheena is a punk rocker she is a punk punk".split(" ")
# MAGIC                 )),
# MAGIC                 key=lambda x: x[0]
# MAGIC             ), 
# MAGIC             lambda x: x[0]
# MAGIC         )
# MAGIC     }.items()
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC {
# MAGIC     r: reduce(
# MAGIC         lambda x, y: x + y, 
# MAGIC         list(map(lambda x: x[1], s))
# MAGIC     )
# MAGIC     for r, s in {
# MAGIC         p: list(v) 
# MAGIC         for p, v in groupby(
# MAGIC             sorted(
# MAGIC                 list(map(
# MAGIC                     lambda x: (x, 1), 
# MAGIC                     "sheena is a punk rocker she is a punk punk".split(" ")
# MAGIC                 )),
# MAGIC                 key=lambda x: x[0]
# MAGIC             ),
# MAGIC             lambda x: x[0]
# MAGIC         )
# MAGIC     }.items()
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC The Python code looks way too complex to be used like this. Normally you would forget functional programming paradigm in this case and code this in a different, more simpler way.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Code is separating words to a list and calculating occurrences for every word and returning a list of them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Cube root
# MAGIC
# MAGIC Write a (recursive) function, `cubeRoot`, that returns an approximate value for the cube root of the input. Use the Newton's method, [https://en.wikipedia.org/wiki/Newton's_method](https://en.wikipedia.org/wiki/Newton%27s_method), with the initial guess of 1. For the cube root this Newton's method translates to:
# MAGIC
# MAGIC $$y_0 = 1$$
# MAGIC $$y_{n+1} = \frac{1}{3}\bigg(2y_n + \frac{x}{y_n^2}\bigg) $$
# MAGIC
# MAGIC where `x` is the input value and `y_n` is the guess for the cube root after `n` iterations.
# MAGIC
# MAGIC Example steps when `x=8`:
# MAGIC
# MAGIC $$y_0 = 1$$
# MAGIC $$y_1 = \frac{1}{3}\big(2*1 + \frac{8}{1^2}\big) = 3.33333$$
# MAGIC
# MAGIC $$y_2 = \frac{1}{3}\big(2*3.33333 + \frac{8}{3.33333^2}\big) = 2.46222$$
# MAGIC
# MAGIC $$y_3 = \frac{1}{3}\big(2*2.46222 + \frac{8}{2.46222^2}\big) = 2.08134$$
# MAGIC
# MAGIC $$...$$
# MAGIC
# MAGIC You will have to decide yourself on what is the condition for stopping the iterations. (you can add parameters to the function if you think it is necessary)
# MAGIC

# COMMAND ----------

def cubeRoot(x: float) -> float:
    if x == 0:
        return 1
    
    return 1/3*(2*cubeRoot(x-1)+x/(cubeRoot(x-1)**2))
    

# COMMAND ----------

def handleCheck(expectedOutput: float, precision: float) -> None:
    inputValue = expectedOutput ** 3
    rootValue = cubeRoot(inputValue)
    if abs(rootValue - expectedOutput) < precision:
        print(f"correct result: {inputValue}^(1/3) == {rootValue}")
    else:
        print(f"wrong result: {rootValue} != {expectedOutput}")

handleCheck(2.0, 1e-6)
handleCheck(3.0, 1e-6)
handleCheck(2023.0, 1e-6)
handleCheck(1.0/42, 1e-6)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - First Spark task
# MAGIC
# MAGIC Create and display a DataFrame with your own data similarly as was done in the tutorial notebook.
# MAGIC
# MAGIC Then fetch the number of rows from the DataFrame.
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame

myData = spark.createDataFrame([
    {"id": 1, "name": "ted"},
    {"id": 2, "name": "barney"},
    {"id": 3, "name": "robin"},
    {"id": 4, "name": "marshall"},
    {"id": 5, "name": "lily"}
])

display(myData)

numberOfRows = myData.count()



# COMMAND ----------

if len(myData) == numberOfRows:
    print("Correct, the data and the DataFrame have the same number of rows.")
else:
    print(f"Wrong, the data has {len(myData)} items while the DataFrame has {numberOfRows} rows.")


# COMMAND ----------


