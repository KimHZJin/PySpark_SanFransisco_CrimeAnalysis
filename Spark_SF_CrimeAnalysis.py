# Databricks notebook source
from csv import reader
from pyspark.sql import Row 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import seaborn as sb
import matplotlib.pyplot as plt
import warnings

import os
os.environ["PYSPARK_PYTHON"] = "python3"

# COMMAND ----------

# import data
import urllib.request
#urllib.request.urlretrieve("https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD", "/tmp/myxxxx.csv")
#dbutils.fs.mv("file:/tmp/myxxxx.csv", "dbfs:/laioffer/spark_hw1/data/sf_06_02.csv")
#display(dbutils.fs.ls("dbfs:/laioffer/spark_hw1/data/"))

# COMMAND ----------

data_path = "dbfs:/laioffer/spark_hw1/data/sf_06_02.csv"

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("CrimeAnalysis") \
    .getOrCreate()

df = spark.read.csv(data_path, header=True, inferSchema=True)
display(df.limit(5))
df.createOrReplaceTempView("sf_crime")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1: Counts the number of crimes for different category.

# COMMAND ----------

q1_df = df.groupBy('category').count().orderBy('count', ascending=False)

display(q1_df)

# COMMAND ----------

import matplotlib.pyplot as plt

pandas_df1 = q1_df.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(pandas_df1['category'], pandas_df['count'], color='skyblue')
plt.xlabel('Category')
plt.ylabel('Count')
plt.title('Crime Counts by Category')
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insight:
# MAGIC It seems that the occurrence thevery tremendously outnumbered other reported crime, with other offences, non-criminal, and assult follow; other crimes shows less difference in reported occurrence in comparison. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2: Counts the number of crimes for different district, and visualize your results

# COMMAND ----------

q2_df = df.groupBy('PdDistrict').count().orderBy('count', ascending=False)

display(q2_df)

# COMMAND ----------

pandas_df2 = q2_df.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(pandas_df2['PdDistrict'], pandas_df2['count'], color='pink')
plt.xlabel('District')
plt.ylabel('Count')
plt.title('Crime Counts by District')
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insights:
# MAGIC First, there is a na column, which should be looked into during the data pre-processing part. 
# MAGIC The southern part of this area is more dangerous than the rest.
# MAGIC The difference of crimes happening between each heighborhood is not so very obvious, but still, Richmond might be a preferable area for residence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3: Count the number of crimes each "Sunday" at "SF downtown".

# COMMAND ----------

from pyspark.sql.functions import hour, date_format, to_date, month, year

df_std = df.withColumn("IncidentDate",to_date(df.Date, "MM/dd/yyyy")) 
sf_downtown = (df_std.X > -122.4313) & (df_std.X < -122.4213) & (df_std.Y < 37.7740) & (df_std.Y > 37.7540 )


# COMMAND ----------

q3_df = df_std.filter((df_std.DayOfWeek == "Sunday") & (sf_downtown)).groupby('IncidentDate','DayOfWeek').count().orderBy('IncidentDate')

# COMMAND ----------

import matplotlib.dates as mdates

q3_pandas_df = q3_df.toPandas()
q3_pandas_df['IncidentDate'] = pd.to_datetime(q3_pandas_df['IncidentDate'])

plt.figure(figsize=(12, 6))
plt.plot(q3_pandas_df['IncidentDate'], q3_pandas_df['count'], marker='o')

plt.xlabel('Incident Date')
plt.ylabel('Count')
plt.title('Number of Crimes on Sundays in Downtown Over Time')
plt.xticks(rotation=45)
plt.grid(True)

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insight:
# MAGIC The general crime count on Sunday seems to increases slightly throughout these years. It appeas that in the middle of each year there were much more crimes reported on Sunday, may I suspect it is due to the National Holiday. Also, in 2013 one Sunday had significantly more crimes happened, which might because of the BART strike or Asiana plane crash which both happened on July.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4: Analysis the number of crime in each month of 2015, 2016, 2017, 2018.

# COMMAND ----------

df_std = df_std.withColumn('Month',month(df_std['IncidentDate']))
df_std = df_std.withColumn('Year', year(df_std['IncidentDate']))

# COMMAND ----------

years = [2015, 2016, 2017, 2018]
df_years = df_std[df_std.Year.isin(years)]
q4_df = df_years.groupby(['Year', 'Month']).count().orderBy('Year','Month')

# COMMAND ----------

q4_pandas_df = q4_df.toPandas()

pivot_df = q4_pandas_df.pivot(index='Month', columns='Year', values='count').fillna(0)
fig, ax = plt.subplots(figsize=(12, 6))

months = pivot_df.index
bar_width = 0.2
years = [2015, 2016, 2017, 2018]
colors = ['b', 'g', 'r', 'c']

r = list(range(len(months)))
for i, year in enumerate(years):
    plt.bar([x + i * bar_width for x in r], pivot_df[year], color=colors[i], width=bar_width, edgecolor='grey', label=year)

plt.xlabel('Month', fontweight='bold')
plt.ylabel('Count', fontweight='bold')
plt.title('Monthly Crime Counts for Selected Years', fontweight='bold')
plt.xticks([r + bar_width for r in range(len(months))], months)
plt.legend(years)

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Insights: 
# MAGIC There are less crimes reported in 2018, while the data from 2018 also seems to be incomplete. There are more crimes happened in 2015 and 2017, while 2016 seems to be a relaticely peaceful year. For business impact, the result suggests that it might be bettr to plan any activities during more peaceful months like the third quarter and avoid some more turbulent months; while holiday months, November and December ought to be peaceful, the trend of the data suggest otherwise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5: Analysis the number of crime with respsect to the hour in certian day like 2015/12/15, 2016/12/15, 2017/12/15. Giving travel suggestion to visit SF.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_std = df_std.withColumn('Date', to_date(df_std['Date'], 'MM/dd/yyyy'))
df_std = df_std.withColumn('IncidentTime', to_timestamp(df_std['Time'], 'HH:mm'))
df_std = df_std.withColumn('Hour', hour(df_std['IncidentTime']))

# COMMAND ----------

dates = ['2015-12-15', '2016-12-15', '2017-12-15']
df_days = df_std.filter(df_std.Date.isin(dates))
q5_df = df_days.groupby('Hour','Date').count().orderBy('Date','Hour')

# COMMAND ----------

q5_pandas_df = q5_df.toPandas()
q5_pandas_df['Date'] = pd.to_datetime(q5_pandas_df['Date'])

plt.figure(figsize=(14, 8))

for date in dates:
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    date_data = q5_pandas_df[q5_pandas_df['Date'] == formatted_date]
    plt.plot(date_data['Hour'], date_data['count'], marker='o', label=formatted_date)

plt.xlabel('Hour of the Day', fontweight='bold')
plt.ylabel('Count of Incidents', fontweight='bold')
plt.title('Number of Incidents by Hour on Specific Dates', fontweight='bold')
plt.xticks(range(0, 24))  
plt.legend(title='Date')

plt.grid(True)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Insights: 
# MAGIC The data suggest that it's better to travel very early in the morning. It also suggests that avoid noon and rush hours, and do not stay outside when it's late. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6:
# MAGIC (1) Step1: Find out the top-3 danger disrict </br>
# MAGIC (2) Step2: find out the crime event w.r.t category and time (hour) from the result of step 1</br>
# MAGIC (3) giving advice to distribute the police based on the analysis results.

# COMMAND ----------

q6_df_s1 = df_std.groupby('PdDistrict').count().orderBy('count',ascending = False)
display(q6_df_s1)

# COMMAND ----------

q6_df_s2 = df_std.filter(df_std.PdDistrict.isin('SOUTHERN', 'MISSION', 'NORTHERN')).groupby('Category','Hour').count().orderBy('Category','Hour')
display(q6_df_s2)

# COMMAND ----------

import seaborn as sns

q6_pandas_df = q6_df_s2.toPandas()

plt.figure(figsize=(16, 10))

sns.lineplot(data=q6_pandas_df, x='Hour', y='count', hue='Category', marker='o')

plt.xlabel('Hour of the Day', fontweight='bold')
plt.ylabel('Count of Incidents', fontweight='bold')
plt.title('Number of Incidents by Hour for Different Categories', fontweight='bold')
plt.xticks(range(0, 24))  
plt.legend(title='Category')

plt.grid(True)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insight:
# MAGIC The suggestion would be to increase the police distribution during 11am - 13pm and 16pm - 21pm. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q7: For different category of crime, find the percentage of resolution. Based on the output, giving hints to adjust the policy.

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

resolution_func = udf (lambda x: x != 'NONE')
q7_df = df_std.withColumn('IsResolution', resolution_func(f.col('Resolution')))

q7_df = q7_df.groupBy('category', 'Resolution', 'IsResolution').count().withColumnRenamed('count', 'resolved').orderBy('category')


display(q7_df)

# COMMAND ----------

q7_df = q7_df.withColumn('total', f.sum('resolved').over(Window.partitionBy('category')))\
    .withColumn('percentage%', f.col('resolved')*100/f.col('total'))\
        .filter(q7_df.IsResolution == True).orderBy('percentage%', ascending=True)

display(q7_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insight:
# MAGIC Crimes with top resolved rates are usually crimes that are caught on the spot; for example, driving under the influence, when the police notice a case, the driver would be dealt with immediately therefore the case would be resolved. On the other hand, the least resolved crimes are also the most frequent ones, like thievery and other offenses. These are usually hard to trace or define, and maybe not serious enough to invest in more resource. The insight is the data reflects the nature of crime and policy. If any adjustment in the policy is needed, maybe more surveillance options can be advised to increase the resolve rate of theft and robbery. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q8: Analysis the new columns of the data and find how to use the new columns (e.g., like 'Fire Prevention Districts' etc)

# COMMAND ----------

q8_df = df.groupBy('Current Police Districts 2 2').count().orderBy('count', ascending=False)

display(q8_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis:
# MAGIC For other columns that we haven't look into before, they can also be used to provide insights about the saftey of San Fransisco. For example, the code above helps to find the most and least safe police districts, and this information can be used to inform which police department needs more assistance. 
