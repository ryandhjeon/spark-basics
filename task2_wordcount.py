import findspark
findspark.init()

text_file = sc.textFile("hdfs:///user/djeon/R06/data/romeojuliet.txt", inferSchema=True, header=True)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://...")