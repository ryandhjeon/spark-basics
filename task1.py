#!/usr/bin/env python
# coding: utf-8

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, mean, min, max, corr
from pyspark.sql.functions import (dayofmonth, hour, dayofyear, month, year, weekofyear, format_number, date_format)
import sys

spark = SparkSession.builder.appName('walmart').getOrCreate()

## Load the Walmart data
df = spark.read.csv('hdfs:///user/djeon/data/SP_500_Historical.csv', inferSchema=True, header=True)
df.columns
df.head()
df.printSchema()

## Print the first 5 rows

for line in df.head(5):
    print(line, '\n')

df.describe().show()

df.printSchema()

## Format columns to show just 2 decimal places

summary = df.describe()
summary.select(summary['summary'],
              format_number(summary['Open'].cast('float'), 2).alias('Open'),
              format_number(summary['High'].cast('float'), 2).alias('High'),
              format_number(summary['Low'].cast('float'), 2).alias('Low'),
              format_number(summary['Close'].cast('float'), 2).alias('Close'),
              format_number(summary['Volume'].cast('float'), 2).alias('Volume'),
              ).show()

## Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day

df_hv = df.withColumn('HV Ratio', df['High']/df['Volume']).select(['HV Ratio'])
df_hv.show()

## Which day has the peak high in price?

df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date']

## What is the mean of the "Close" column

df.select(mean('Close')).show()

## What is the maximum and minimum value of the "Volumn" column

df.select(max('Volume'), min('Volume')).show()

## How many days did the stocks close lower than 60 dollars?

df.filter(df['Close'] < 60).count()

## What percentage of the time was the "High" greater than 80 dollars?

df.filter('High > 80').count() * 100/df.count()

## What is the Pearson correlation between "High" and "Volume"

df.corr('High', 'Volume')

df.select(corr(df['High'], df['Volume'])).show()

## What is the max "High" per year?

year_df = df.withColumn('Year', year(df['Date']))

year_df.groupBy('Year').max()['Year','max(High)'].show()

## What is the average 'Close' for each calender month?

# New column Month
month_df = df.withColumn('Month', month(df['Date']))

# Group by month and take average of all other columns
month_df = month_df.groupBy('Month').mean()

# Sort by month
month_df = month_df.orderBy('Month')

#Display only month and avg(Close), the desired columns
month_df['Month', 'avg(Close)'].show()