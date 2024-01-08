# Databricks notebook source
# MAGIC %md
# MAGIC # Data-Intensive Programming - Assignment
# MAGIC
# MAGIC This is the **Python** version of the assignment. Switch to the Scala version, if you want to do the assignment in Scala.
# MAGIC
# MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add new cells if you want.
# MAGIC
# MAGIC Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.
# MAGIC
# MAGIC ## Basic tasks (compulsory)
# MAGIC
# MAGIC There are in total seven basic tasks that every group must implement in order to have an accepted assignment.
# MAGIC
# MAGIC The basic task 1 is a warming up task and it deals with some video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.
# MAGIC
# MAGIC The other basic tasks (basic tasks 2-7) are all related and deal with data from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) that contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Knowledge about ice hockey or NHL is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.
# MAGIC
# MAGIC ## Additional tasks (optional, can provide course points)
# MAGIC
# MAGIC There are in total of three additional tasks that can be done to gain some course points.
# MAGIC
# MAGIC The first additional task asks you to do all the basic tasks in an optimized way. It is possible that you can some points from this without directly trying by just implementing the basic tasks in an efficient manner.
# MAGIC
# MAGIC The other two additional tasks are separate tasks and do not relate to any other basic or additional tasks. One of them asks you to load in unstructured text data and do some calculations based on the words found from the data. The other asks you to utilize the K-Means algorithm to partition the given building data.
# MAGIC
# MAGIC It is possible to gain partial points from the additional tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task.
# MAGIC

# COMMAND ----------

# import statements for the entire notebook
# add anything that is required here

import math
from typing import List
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 1 - Sales data
# MAGIC
# MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (from [https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data](https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data)). The direct address for the dataset is: `abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv`
# MAGIC
# MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset.
# MAGIC
# MAGIC Only data for sales in the first ten years of the 21st century should be considered in this task, i.e. years 2000-2009.
# MAGIC
# MAGIC Using the data, find answers to the following:
# MAGIC
# MAGIC - Which publisher had the highest total sales in video games in European Union in years 2000-2009?
# MAGIC - What were the total yearly sales, in European Union and globally, for this publisher in year 2000-2009
# MAGIC

# COMMAND ----------

gameSales: DataFrame = spark.read.options(
    header = True,
    inferSchema = True
    ).csv('abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv')

bestEUPublisher: str = gameSales.filter(F.col('Year').between(2000,2009)).groupBy('Publisher').agg(
    F.sum('EU_Sales').alias('EU_total_sales')
).orderBy(F.desc('EU_total_sales')).select('Publisher').first()[0]

bestEUPublisherSales: DataFrame = gameSales.filter(F.col('Publisher') == bestEUPublisher).filter(
    F.col('Year').between(2000, 2009)
).groupBy('Year').agg(
    F.round(F.sum('EU_Sales'), 2).alias('EU_Total'),
    F.round(F.sum('Global_Sales'), 2).alias('Global_Total')
).orderBy(F.asc('Year'))

print(f"The publisher with the highest total video game sales in European Union is: '{bestEUPublisher}'")
print("Sales data for the publisher:")
bestEUPublisherSales.show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 2 - Shot data from NHL matches
# MAGIC
# MAGIC A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/nhl_shots.parquet` from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23.
# MAGIC
# MAGIC In this task you should load the data with all of the rows into a data frame. This data frame object will then be used in the following basic tasks 3-7.
# MAGIC
# MAGIC ### Background information
# MAGIC
# MAGIC Each NHL season is divided into regular season and playoff season. In the regular season the teams play up to 82 games with the best teams continuing to the playoff season. During the playoff season the remaining teams are paired and each pair play best-of-seven series of games to determine which team will advance to the next phase.
# MAGIC
# MAGIC In ice hockey each game has a home team and an away team. The regular length of a game is three 20 minute periods, i.e. 60 minutes or 3600 seconds. The team that scores more goals in the regulation time is the winner of the game.
# MAGIC
# MAGIC If the scoreline is even after this regulation time:
# MAGIC
# MAGIC - In playoff games, the game will be continued until one of the teams score a goal with the scoring team being the winner.
# MAGIC - In regular season games, there is an extra time that can last a maximum of 5 minutes (300 seconds). If one of the teams score, the game ends with the scoring team being the winner. If there is no goals in the extra time, there would be a shootout competition to determine the winner. These shootout competitions are not considered in this assignment, and the shots from those are not included in the raw data.
# MAGIC
# MAGIC **Columns in the data**
# MAGIC
# MAGIC Each row in the given data represents one shot in a game.
# MAGIC
# MAGIC The column description from the source website. Not all of these will be needed in this assignment.
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | shotID      | integer | Unique id for each shot |
# MAGIC | homeTeamCode | string | The home team in the game. For example: TOR, MTL, NYR, etc. |
# MAGIC | awayTeamCode | string | The away team in the game |
# MAGIC | season | integer | Season the shot took place in. Example: 2009 for the 2009-2010 season |
# MAGIC | isPlayOffGame | integer | Set to 1 if a playoff game, otherwise 0 |
# MAGIC | game_id | integer | The NHL Game_id of the game the shot took place in |
# MAGIC | time | integer | Seconds into the game of the shot |
# MAGIC | period | integer | Period of the game |
# MAGIC | team | string | The team taking the shot. HOME or AWAY |
# MAGIC | location | string | The zone the shot took place in. HOMEZONE, AWAYZONE, or Neu. Zone |
# MAGIC | event | string | Whether the shot was a shot on goal (SHOT), goal, (GOAL), or missed the net (MISS) |
# MAGIC | homeTeamGoals | integer | Home team goals before the shot took place |
# MAGIC | awayTeamGoals | integer | Away team goals before the shot took place |
# MAGIC | homeTeamWon | integer | Set to 1 if the home team won the game. Otherwise 0. |
# MAGIC | shotType | string | Type of the shot. (Slap, Wrist, etc) |
# MAGIC

# COMMAND ----------

source_parquet = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/nhl_shots.parquet"
shotsDF: DataFrame = spark.read.parquet(source_parquet)

# why are there same game_ids in different seasons?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 3 - Game data frame
# MAGIC
# MAGIC Create a match data frame for all the game included in the shots data frame created in basic task 2.
# MAGIC
# MAGIC The output should contain one row for each game.
# MAGIC
# MAGIC The following columns should be included in the final data frame for this task:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | Season the game took place in. Example: 2009 for the 2009-2010 season |
# MAGIC | game_id        | integer     | The NHL Game_id of the game |
# MAGIC | homeTeamCode   | string      | The home team in the game. For example: TOR, MTL, NYR, etc. |
# MAGIC | awayTeamCode   | string      | The away team in the game |
# MAGIC | isPlayOffGame  | integer     | Set to 1 if a playoff game, otherwise 0 |
# MAGIC | homeTeamGoals  | integer     | Number of goals scored by the home team |
# MAGIC | awayTeamGoals  | integer     | Number of goals scored by the away team |
# MAGIC | lastGoalTime   | integer     | The time in seconds for the last goal in the game. 0 if there was no goals in the game. |
# MAGIC
# MAGIC All games had at least some shots but there are some games that did not have any goals either in the regulation 60 minutes or in the extra time.
# MAGIC
# MAGIC Note, that for a couple of games there might be some shots, including goal-scoring ones, that are missing from the original dataset. For example, there might be a game with a final scoreline of 3-4 but only 6 of the goal-scoring shots are included in the dataset. Your solution does not have to try to take these rare occasions of missing data into account. I.e., you can do all the tasks with the assumption that there are no missing or invalid data.
# MAGIC

# COMMAND ----------

gamesDF: DataFrame = shotsDF.groupBy('season', 'game_id').agg(
    F.first('homeTeamCode').alias('homeTeamCode'),
    F.first('awayTeamCode').alias('awayTeamCode'),
    F.first('isPlayOffGame').alias('isPlayOffGame'),
    F.max('homeTeamGoals').alias('homeTeamGoals'),
    F.max('awayTeamGoals').alias('awayTeamGoals'),
    F.max(F.when(F.col('event') == 'GOAL', F.col('time')).otherwise(0)).alias('lastGoalTime')
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 4 - Game wins during playoff seasons
# MAGIC
# MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated number of wins and losses for each team and for each playoff season, i.e. for games which have been marked as playoff games. Only teams that have played in at least one playoff game in the considered season should be included in the final data frame.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of playoff games the team played in the given season |
# MAGIC | wins           | integer     | Number of wins in playoff games the team had in the given season |
# MAGIC | losses         | integer     | Number of losses in playoff games the team had in the given season |
# MAGIC
# MAGIC Playoff games where a team scored more goals than their opponent are considered winning games. And playoff games where a team scored less goals than the opponent are considered losing games.
# MAGIC
# MAGIC In real life there should not be any playoff games where the final score line was even but due to some missing shot data you might end up with a couple of playoff games that seems to have ended up in a draw. These possible "drawn" playoff games can be left out from the win/loss calculations.
# MAGIC

# COMMAND ----------

playoffDF: DataFrame = gamesDF.filter(gamesDF['isPLayOffGame'] == 1)

playoff_home = playoffDF.groupBy('season', 'homeTeamCode').agg(
    F.count('game_id').alias('games'),
    F.sum(F.when(playoffDF['homeTeamGoals'] > playoffDF['awayTeamGoals'], 1).otherwise(0)).alias('wins'),
    F.sum(F.when(playoffDF['homeTeamGoals'] < playoffDF['awayTeamGoals'], 1).otherwise(0)).alias('losses')
).withColumnRenamed('homeTeamCode', 'teamCode')

playoff_away = playoffDF.groupBy('season', 'awayTeamCode').agg(
    F.count('game_id').alias('games'),
    F.sum(F.when(playoffDF['homeTeamGoals'] < playoffDF['awayTeamGoals'], 1).otherwise(0)).alias('wins'),
    F.sum(F.when(playoffDF['homeTeamGoals'] > playoffDF['awayTeamGoals'], 1).otherwise(0)).alias('losses')
).withColumnRenamed('awayTeamCode', 'teamCode')

playoffDF = playoff_home.union(playoff_away).groupBy('season', 'teamCode').agg(
    F.sum('games').alias('games'),
    F.sum('wins').alias('wins'),
    F.sum('losses').alias('losses')
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 5 - Best playoff teams
# MAGIC
# MAGIC Using the playoff data frame created in basic task 4 create a data frame containing the win-loss record for best playoff team, i.e. the team with the most wins, for each season. You can assume that there are no ties for the highest amount of wins in each season.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The team code for the best performing playoff team in the given season. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of playoff games the best performing playoff team played in the given season |
# MAGIC | wins           | integer     | Number of wins in playoff games the best performing playoff team had in the given season |
# MAGIC | losses         | integer     | Number of losses in playoff games the best performing playoff team had in the given season |
# MAGIC
# MAGIC Finally, fetch the details for the best playoff team in season 2022.
# MAGIC

# COMMAND ----------

w = Window.partitionBy('season')
bestPlayoffTeams = playoffDF.withColumn(
    'winsMax', F.max('wins').over(w)
    ).filter(F.col('wins') == F.col('winsMax')).drop('winsMax')


# COMMAND ----------

bestPlayoffTeam2022: Row = bestPlayoffTeams.filter(F.col('season') == 2022).first()

bestPlayoffTeam2022Dict: dict = bestPlayoffTeam2022.asDict()
print("Best playoff team in 2022:")
print(f"    Team: {bestPlayoffTeam2022Dict.get('teamCode')}")
print(f"    Games: {bestPlayoffTeam2022Dict.get('games')}")
print(f"    Wins: {bestPlayoffTeam2022Dict.get('wins')}")
print(f"    Losses: {bestPlayoffTeam2022Dict.get('losses')}")
print("=========================================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 6 - Regular season points
# MAGIC
# MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated data for each team and for each season for the regular season matches, i.e. the non-playoff matches.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of non-playoff games the team played in the given season |
# MAGIC | wins           | integer     | Number of wins in non-playoff games the team had in the given season |
# MAGIC | losses         | integer     | Number of losses in non-playoff games the team had in the given season |
# MAGIC | goalsScored    | integer     | Total number goals scored by the team in non-playoff games in the given season |
# MAGIC | goalsConceded  | integer     | Total number goals scored against the team in non-playoff games in the given season |
# MAGIC | points         | integer     | Total number of points gathered by the team in non-playoff games in the given season |
# MAGIC
# MAGIC Points from each match are received as follows (in the context of this assignment, these do not exactly match the NHL rules):
# MAGIC
# MAGIC | points | situation |
# MAGIC | ------ | --------- |
# MAGIC | 3      | team scored more goals than the opponent during the regular 60 minutes |
# MAGIC | 2      | the score line was even after 60 minutes but the team scored a winning goal during the extra time |
# MAGIC | 1      | the score line was even after 60 minutes but the opponent scored a winning goal during the extra time or there were no goals in the extra time |
# MAGIC | 0      | the opponent scored more goals than the team during the regular 60 minutes |
# MAGIC
# MAGIC In the regular season the following table shows how wins and losses should be considered (in the context of this assignment):
# MAGIC
# MAGIC | win | loss | situation |
# MAGIC | --- | ---- | --------- |
# MAGIC | Yes | No   | team gained at least 2 points from the match |
# MAGIC | No  | Yes  | team gain at most 1 point from the match |
# MAGIC

# COMMAND ----------

regularSeasonDF: DataFrame = gamesDF.filter(F.col('isPlayOffGame') == 0)

regularSeasonDF_home = regularSeasonDF.groupBy('season', 'homeTeamCode').agg(
    F.count('game_id').alias('games'),
    F.sum(F.when(F.col('homeTeamGoals') > F.col('awayTeamGoals'), 1).otherwise(0)).alias('wins'),
    F.sum(F.when(F.col('homeTeamGoals') < F.col('awayTeamGoals'), 1).otherwise(0)).alias('losses'),
    F.sum(F.col('homeTeamGoals')).alias('goalsScored'),
    F.sum(F.col('awayTeamGoals')).alias('goalsConceded'),
    F.sum(F.when(
        (F.col('homeTeamGoals') > F.col('awayTeamGoals')) & 
        (F.col('lastGoalTime') <= 60*60),
        3
    ).when(
        (F.col('homeTeamGoals') > F.col('awayTeamGoals')) &
        (F.col('lastGoalTime') > 60*60),
        2
    ).when(
        (F.col('homeTeamGoals') < F.col('awayTeamGoals')) &
        (F.col('lastGoalTime') > 60*60),
        1
    ).otherwise(0)).alias('points')
).withColumnRenamed('homeTeamCode', 'teamCode')

regularSeasonDF_away = regularSeasonDF.groupBy('season', 'awayTeamCode').agg(
    F.count('game_id').alias('games'),
    F.sum(F.when(F.col('homeTeamGoals') < F.col('awayTeamGoals'), 1).otherwise(0)).alias('wins'),
    F.sum(F.when(F.col('homeTeamGoals') > F.col('awayTeamGoals'), 1).otherwise(0)).alias('losses'),
    F.sum(F.col('awayTeamGoals')).alias('goalsScored'),
    F.sum(F.col('homeTeamGoals')).alias('goalsConceded'),
    F.sum(F.when(
        (F.col('homeTeamGoals') < F.col('awayTeamGoals')) & 
        (F.col('lastGoalTime') <= 60*60),
        3
    ).when(
        (F.col('homeTeamGoals') < F.col('awayTeamGoals')) &
        (F.col('lastGoalTime') > 60*60),
        2
    ).when(
        (F.col('homeTeamGoals') > F.col('awayTeamGoals')) &
        (F.col('lastGoalTime') > 60*60),
        1
    ).otherwise(0)).alias('points')
).withColumnRenamed('awayTeamCode', 'teamCode')

regularSeasonDF = regularSeasonDF_home.union(regularSeasonDF_away).groupBy('season', 'teamCode').agg(
    F.sum('games').alias('games'),
    F.sum('wins').alias('wins'),
    F.sum('losses').alias('losses'),
    F.sum(F.col('goalsScored')).alias('goalsScored'),
    F.sum(F.col('goalsConceded')).alias('goalsConceded'),
    F.sum(F.col('points')).alias('points')
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 7 - The worst regular season teams
# MAGIC
# MAGIC Using the regular season data frame created in the basic task 6, create a data frame containing the regular season records for the worst regular season team, i.e. the team with the least amount of points, for each season. You can assume that there are no ties for the lowest amount of points in each season.
# MAGIC
# MAGIC Finally, fetch the details for the worst regular season team in season 2022.
# MAGIC

# COMMAND ----------

w = Window.partitionBy('season')

worstRegularTeams: DataFrame = regularSeasonDF.withColumn(
    'pointsMin', F.min('points').over(w)
    ).filter(F.col('points') == F.col('pointsMin')).drop('pointsMin')


# COMMAND ----------

worstRegularTeam2022: Row = worstRegularTeams.filter(F.col('season') == 2022).first()

worstRegularTeam2022Dict: dict = worstRegularTeam2022.asDict()
print("Worst regular season team in 2022:")
print(f"    Team: {worstRegularTeam2022Dict.get('teamCode')}")
print(f"    Games: {worstRegularTeam2022Dict.get('games')}")
print(f"    Wins: {worstRegularTeam2022Dict.get('wins')}")
print(f"    Losses: {worstRegularTeam2022Dict.get('losses')}")
print(f"    Goals scored: {worstRegularTeam2022Dict.get('goalsScored')}")
print(f"    Goals conceded: {worstRegularTeam2022Dict.get('goalsConceded')}")
print(f"    Points: {worstRegularTeam2022Dict.get('points')}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional tasks
# MAGIC
# MAGIC The implementation of the basic tasks is compulsory for every group.
# MAGIC
# MAGIC Doing the following additional tasks you can gain course points which can help in getting a better grade from the course (or passing the course).
# MAGIC Partial solutions can give partial points.
# MAGIC
# MAGIC The additional task 1 will be considered in the grading for every group based on their solutions for the basic tasks.
# MAGIC
# MAGIC The additional tasks 2 and 3 are separate tasks that do not relate to any other task in the assignment. The solutions used in these other additional tasks do not affect the grading of additional task 1. Instead, a good use of optimized methods can positively affect the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 1 - Optimized solutions to the basic tasks (2 points)
# MAGIC
# MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
# MAGIC
# MAGIC A couple of things to consider (**NOT** even close to a complete list):
# MAGIC
# MAGIC - Consider using explicit schemas when dealing with CSV data sources.
# MAGIC - Consider only including those columns from a data source that are actually needed.
# MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
# MAGIC - Avoid collect or similar expensive operations for large datasets.
# MAGIC - Consider using explicit caching if some data frame is used repeatedly.
# MAGIC - Avoid unnecessary shuffling (for example sorting) operations.
# MAGIC
# MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
# MAGIC
# MAGIC Note, that you should not do the basic tasks again for this additional task, but instead modify your basic task code with more efficient versions.
# MAGIC
# MAGIC You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Initally, we used as effective solutions as possible or modified them along the way of implementing each solution.
# MAGIC
# MAGIC For example excessive sorting in picking the best and worst teams for each season was cut out. We provided a solution where we could use .max() from functions to find the best and worst teams.
# MAGIC
# MAGIC In task 5. we did at first an implementation where we created a new dataframe with column 'rank' which saved the ranking for each team based on amount of wins. The solution now in task is more optimized since it doesn't create a temporary dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 2 - Unstructured data (2 points)
# MAGIC
# MAGIC You are given some text files with contents from a few thousand random articles both in English and Finnish from Wikipedia. Content from English articles are in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/wikipedia/en` and content from Finnish articles are at folder `assignment/wikipedia/fi`.
# MAGIC
# MAGIC Some cleaning operations have already been done to the texts but the some further cleaning is still required.
# MAGIC
# MAGIC The final goal of the task is to get the answers to following questions:
# MAGIC
# MAGIC - What are the ten most common English words that appear in the English articles?
# MAGIC - What are the five most common 5-letter Finnish words that appear in the Finnish articles?
# MAGIC - What is the longest word that appears at least 150 times in the articles?
# MAGIC - What is the average English word length for the words appearing in the English articles?
# MAGIC - What is the average Finnish word length for the words appearing in the Finnish articles?
# MAGIC
# MAGIC For a word to be included in the calculations, it should fulfill the following requirements:
# MAGIC
# MAGIC - Capitalization is to be ignored. I.e., words "English", "ENGLISH", and "english" are all to be considered as the same word "english".
# MAGIC - An English word should only contain the 26 letters from the alphabet of Modern English. Only exception is that punctuation marks, i.e. hyphens `-`, are allowed in the middle of the words as long as there are no two consecutive punctuation marks without any letters between them.
# MAGIC - The only allowed 1-letter English words are `a` and `i`.
# MAGIC - A Finnish word should follow the same rules as English words, except that three additional letters, `å`, `ä`, and `ö`, are also allowed, and that no 1-letter words are allowed. Also, any word that contains "`wiki`" should not be considered as Finnish word.
# MAGIC
# MAGIC Some hints:
# MAGIC
# MAGIC - Using an RDD or a Dataset (in Scala) might make the data cleaning and word determination easier than using DataFrames.
# MAGIC - It can be assumed that in the source data each word in the same line is separated by at least one white space (` `).
# MAGIC - You are allowed to remove all non-allowed characters from the source data at the beginning of the cleaning process.
# MAGIC - It is advisable to first create a DataFrame/Dataset/RDD that contains the found words, their language, and the number of times those words appeared in the articles. This can then be used as the starting point when determining the answers to the given questions.
# MAGIC

# COMMAND ----------

# some constants that could be useful
englishLetters: str = "abcdefghijklmnopqrstuvwxyz"
finnishLetters: str = englishLetters + "åäö"
whiteSpace: str = " "
punctuationMark: str = '-'
twoPunctuationMarks: str = "--"
allowedEnglishOneLetterWords: List[str] = ["a", "i"]
wikiStr: str = "wiki"

englishStr: str = "English"
finnishStr: str = "Finnish"


# COMMAND ----------

source_txt_fileEn = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/wikipedia/en/*.txt"
source_txt_fileFi = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/wikipedia/fi/*.txt"

rddEnFromFile = spark.sparkContext.textFile(source_txt_fileEn)
rddFiFromFile = spark.sparkContext.textFile(source_txt_fileFi)

print("Lines in English articles:", rddEnFromFile.count())
print("Lines in Finnish articles:" , rddFiFromFile.count())


# COMMAND ----------

def cleanEn(text):
    allowedEnglishOneLetterWords: List[str] = ["a", "i"]
    punctuationMark: str = '-'
    words = []
    for i in text.split(" "):
       if (len(i) > 1 and i.isalpha()) or (len(i) == 1 and i in allowedEnglishOneLetterWords):
           words.append(i.lower())
    return words

cleaned_en = rddEnFromFile.flatMap(cleanEn).filter(lambda x: x != [])

def isAlphaFi(text):
    englishLetters: str = "abcdefghijklmnopqrstuvwxyz"
    finnishLetters: str = englishLetters + "åäö"
    for i in text.lower():
        if i not in finnishLetters:
            return False
    return True

def cleanFi(text):
    wikiStr: str = "wiki"
    words = []
    for i in text.split(" "):
        if len(i) > 1 and isAlphaFi(i):
            words.append(i.lower())
    return words

cleaned_fi = rddFiFromFile.flatMap(cleanFi).filter(lambda x: x!= [] and wikiStr not in x)

print("English words after cleaning: ",cleaned_en.count())
print("Finnish words after cleaning: ", cleaned_fi.count())



# COMMAND ----------

word_counts_en = cleaned_en.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)

commonWordsEn: DataFrame = spark.createDataFrame(word_counts_en, ['Word', 'Count']).orderBy(F.desc(F.col('Count'))).limit(10)

print("The ten most common English words that appear in the English articles:")
commonWordsEn.show()


# COMMAND ----------

words_count_fi = cleaned_fi.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)
words_count_fi_5letter = words_count_fi.filter(lambda word: len(word[0]) == 5)

common5LetterWordsFi: DataFrame = spark.createDataFrame(words_count_fi_5letter, ['Word', 'Count']).orderBy(F.desc(F.col('Count'))).limit(5)

print("The five most common 5-letter Finnish words that appear in the Finnish articles:")
common5LetterWordsFi.show()


# COMMAND ----------

longestEn = word_counts_en.filter(lambda word: word[1] >= 150).max(lambda word: len(word[0]))
longestFi = words_count_fi.filter(lambda word: word[1] >= 150).max(lambda word: len(word[0]))

longestWord: str = longestEn[0] if len(longestEn[0]) > len(longestFi[0]) else longestFi[0]

print(f"The longest word appearing at least 150 times is '{longestWord}'")

avgWordLenEn =  round(cleaned_en.map(lambda word: len(word)).mean(), 2)
avgWordLenFi = round(cleaned_fi.map(lambda word: len(word)).mean(), 2)

# create dataframe
avgData = [
    Row(language=finnishStr, average_word_lenght=avgWordLenFi),
    Row(language=englishStr, average_word_lenght=avgWordLenEn)
]

averageWordLengths: DataFrame = spark.createDataFrame(avgData)

print("The average word lengths:")
averageWordLengths.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 3 - K-Means clustering (2 points)
# MAGIC
# MAGIC You are given a dataset containing the locations of building in Finland. The dataset is a subset from [https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f](https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f) limited to only postal codes with the first two numbers in the interval 30-44 ([postal codes in Finland](https://www.posti.fi/en/zip-code-search/postal-codes-in-finland)). The dataset is in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/buildings.parquet`.
# MAGIC
# MAGIC [K-Means clustering](https://en.wikipedia.org/wiki/K-means_clustering) algorithm is an unsupervised machine learning algorithm that can be used to partition the input data into k clusters. Your task is to use the Spark ML library and its K-Means clusterization algorithm to divide the buildings into clusters using the building coordinates `latitude_wgs84` and `longitude_wgs84` as the basis of the clusterization. You should implement the following procedure:
# MAGIC
# MAGIC 1. Start with all the buildings in the dataset.
# MAGIC 2. Divide the buildings into seven clusters with K-Means algorithm using `k=7` and the longitude and latitude of the buildings.
# MAGIC 3. Find the cluster to which the Sähkötalo building from the Hervanta campus is sorted into. The building id for Sähkötalo in the dataset is `102363858X`.
# MAGIC 4. Choose all the buildings from the cluster with the Sähkötalo building.
# MAGIC 5. Find the cluster center for the chosen cluster of buildings.
# MAGIC 6. Calculate the largest distance from a building in the chosen cluster to the chosen cluster center. You are given a function `haversine` that you can use to calculate the distance between two points using the latitude and longitude of the points.
# MAGIC 7. While the largest distance from a building in the considered cluster to the cluster center is larger than 3 kilometers run the K-Means algorithm again using the following substeps.
# MAGIC     - Run the K-Means algorithm to divide the remaining buildings into smaller clusters. The number of the new clusters should be one less than in the previous run of the algorithm (but should always be at least two). I.e., the sequence of `k` values starting from the second run should be 6, 5, 4, 3, 2, 2, ...
# MAGIC     - After using the algorithm again, choose the new smaller cluster of buildings so that it includes the Sähkötalo building.
# MAGIC     - Find the center of this cluster and calculate the largest distance from a building in this cluster to its center.
# MAGIC
# MAGIC As the result of this process, you should get a cluster of buildings that includes the Sähkötalo building and in which all buildings are within 3 kilometers of the cluster center.
# MAGIC
# MAGIC Using the final cluster, find the answers to the following questions:
# MAGIC
# MAGIC - How many buildings in total are in the final cluster?
# MAGIC - How many Hervanta buildings are in this final cluster? (A building is considered to be in Hervanta if their postal code is `33720`)
# MAGIC
# MAGIC Some hints:
# MAGIC
# MAGIC - Once you have trained a KMeansModel, the coordinates for the cluster centers, and the cluster indexes for individual buildings can be accessed through the model object (`clusterCenters`, `summary.predictions`).
# MAGIC - The given haversine function for calculating distances can be used with data frames if you turn it into an user defined function.
# MAGIC

# COMMAND ----------

# some helpful constants
startK: int = 7
seedValue: int = 1

# the building id for Sähkötalo building at Hervanta campus
hervantaBuildingId: str = "102363858X"
hervantaPostalCode: int = 33720

maxAllowedClusterDistance: float = 3.0


# returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
# based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R: float = 6378.1  # radius of Earth in kilometers
    print(lat1)
    print(lon1)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    deltaPhi = math.radians(lat2 - lat1)
    deltaLambda = math.radians(lon2 - lon1)

    a = (
        math.sin(deltaPhi * deltaPhi / 4.0) +
        math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
    )

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# COMMAND ----------

source_file = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/buildings.parquet"

buildingDF: DataFrame = spark.read.parquet(source_file)

assembler = VectorAssembler(inputCols=['latitude_wgs84', 'longitude_wgs84'], outputCol='features')
dataset = assembler.transform(buildingDF)

counter = 0
i = startK
cluster = dataset

while i >= 2:

    building_count = cluster.count()
    counter += 1

    # Create model and calculate predictions
    kmeans = KMeans(k=i, seed=seedValue)
    model = kmeans.fit(cluster)

    predictions = model.transform(cluster)

    # Find cluster containing 'Sähkötalo'
    cluster_id = predictions.filter(F.col('building_id') == hervantaBuildingId).select('prediction').collect()[0][0]
    cluster = predictions.filter(F.col('prediction') == cluster_id)

    # Get Sähkötalo cluster's center
    center = model.clusterCenters()[cluster_id]

    # Calculate new cluster
    haversine_udf = F.udf(haversine) # turn to udf to use in withColumn

    cluster = cluster.withColumn('distance', haversine_udf('latitude_wgs84', 'longitude_wgs84', F.lit(center[0]), F.lit(center[1])).cast('double'))
    max_dist = float(cluster.select(F.max('distance')).collect()[0][0])

    # Drop distance and prediction from current cluster
    cluster = cluster.drop('distance').drop('prediction')

    # Print iteration information
    print(f"(k={i}, iteration={counter}) Buildings: {building_count} -> {cluster.count()}, Maximum distance to the center within 'Sähkötalo' cluster: {round(max_dist, 2)} km")

    if i > 2:
        i = i - 1
    if not max_dist > maxAllowedClusterDistance:
        cluster.drop('features')
        break

finalCluster: DataFrame = cluster

clusterBuildingCount: int = cluster.count()
clusterHervantaBuildingCount: int = cluster.filter(F.col('postal_code') == hervantaPostalCode).count()

print(f"Buildings in the final cluster: {clusterBuildingCount}")
print(f"Hervanta buildings in the final cluster: {clusterHervantaBuildingCount} ", end="")
print(f"({round(100*clusterHervantaBuildingCount/clusterBuildingCount, 2)}% of all buildings in the final cluster)")
print("===========================================================================================")


# COMMAND ----------


