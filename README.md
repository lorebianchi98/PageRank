# Page Rank

Firstly be sure that 'wiki-micro.txt' is present in HDFS with:
```
hadoop fs -ls
```

## How to execute the Spark Page Rank application

Then, in the folder where the 'PageRank_Spark.py' is present execute:
```
$SPARK_HOME/bin/spark-submit PageRank_Spark.py <input> <output> <iteration> <alpha>
```
With input 'wiki-micro.txt', output will be the folder in which the program will write the results 
divided into different file 'part-0000x' inside the output folder. To check result:
```
hadoop fs -cat output/part-0000x 
```

## How to execute the Hadoop Page Rank application

In the PageRank folder execute

```
mvn clean package
```

Then:
```
 hadoop jar target/PageRank-1.0-SNAPSHOT.jar it.unipi.hadoop.PageRank <input> <output directory> <# of iterations> <# of reducers> <random jump probability>
```
You will retrieve the output in the ouput directory.
