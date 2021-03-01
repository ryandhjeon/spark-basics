# Spark Basics

__What commands must be used to run your scripts? Which Spark examples did you run?__

### Task1 

Client command:
`spark-submit --deploy-mode client task1.py hdfs:///user/djeon/data/SP_500_Historical.csv.txt > ./output/output_task1_client.txt`

### WordCount

Client command:

`spark-submit --deploy-mode client /home/hadoop/spark/examples/src/main/python/wordcount.py hdfs:///user/djeon/data/romeojuliet.txt > ./output/output_task2_wordcount.txt`

Cluster command: 

`spark-submit --deploy-mode cluster /home/hadoop/spark/examples/src/main/python/wordcount.py hdfs:///user/djeon/data/romeojuliet.txt`

Yarn log command: 

`yarn logs -applicationId application_1614374698785_0627`

### pi

Client command:
`spark-submit --deploy-mode client /home/hadoop/spark/examples/src/main/python/pi.py > ./output_task2_pi.txt`

Cluster command: 

`spark-submit --deploy-mode cluster /home/hadoop/spark/examples/src/main/python/pi.py`

Yarn log command: 

`yarn logs -applicationId application_1614374698785_0715`

---

__What technical errors did you experience? Please list them explicitly and identify how you corrected them.__

The Jupyter notebook won't open! I figured out that I had both Physical space and docker opened in the same port. One of them had to be closed. Learned it the hard way.  

__What conceptual difficulties did you experience?__

It wasn't too difficult so far, but I'm sure they'll come anytime soon.

__How much time did you spend on each part of the assignment?__

Task #1: 6 hours for setting up and doing tutorials.

Task #2: 3~4 hours.

__Track your time according to the following items: Gitlab & Git, Docker setup/usage, actual reflection work, etc.__

Gitlab & Git: 5 min

Docker setup/usage: 30 min. I had to delete and reinstall Docker

Actual reflection work: 9~10 hours. 

__What was the hardest part of this assignment?__

There are so much information to learn and take as mine. I will have to continuously utilizing and learn about the Spar.

__What was the easiest part of this assignment?__

Following tutorials for the Walmart exercise.

__What advice would you give someone doing this assignment in the future?__

I would tell them that make sure to read the Spark documents, and learn how the Spark works in theory. It will make much more sense when you read and watch the tutorial videos on them. There are a lot of good tutorials out there, as Spark is a well known framework that everyone uses now.

__What did you actually learn from doing this assignment?__

I learned how mapreduce and hadoop is old compared to Spark in terms of speed and other useful features. 

__Why does what I learned matter both academically and practically?__

As a Data Science student, I have to realize that what is trending in the practical world. Spark is one of the most used framework out there that outlasts any competitors in a Big data field. There must be a reason behind it. As I learn about the usage of Spark, it is becoming a must use for managing big data or doing machine learning tasks.