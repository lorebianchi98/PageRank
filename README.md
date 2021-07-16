# Page Rank

## How to execute the Spark Page Rank application

Firstly be sure that 'wiki-micro.txt' is present in HDFS with:
```
hadoop fs -ls
```
Then, in the folder where the 'PageRank_Spark.py' is present execute:
```
$SPARK_HOME/bin/spark-submit PageRank_Spark.py <input> <output> <iteration> <alpha>
```
With input 'wiki-micro.txt', output will be the folder in which the program will write the results 
dividided into different file 'part-0000x' inside the output folder. To check result:
```
hadoop fs -cat output/part-0000x 
```

