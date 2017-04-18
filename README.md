# struct-streaming-quotes-injector

Package for Saagie : sbt clean assembly and get the package in target

To pass args to the program we use scopt. Scopt is a little command line options parsing library.

* triggerTime: Time between batches. Default 0ms
* hdfsNameNodeHost: URI of hdfs nameNode. Example : hdfs://IP:8020
* hdfsPathCheckpoint: Directory path where the checkpoiting data are writen. Example: /user/spark/checkpointing/injector
* hdfsPathQuotes: Directory path where the quotes data are writen : Example: /user/hdfs/quotes
* kafkaBrokerUrl: Url of kafka broker : Example: localhost:9092
* kafkaTopic: Name of your quote topics

Usage in local :

* sbt clean assembly
* for price average : spark-submit {driver_options} --class=io.saagie.quotes.QuotesVariation --executor-cores 2 --executor-memory 6g --driver-memory 4g --conf "spark.cores.max=4" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+UseCompressedOops" {file} --hdfsNameNodeHost hdfs://nn1.p11.prod.saagie.io:8020 --hdfsPathCheckpoint your-checkpoint-path --hdfsPathQuotes your-data-path --kafkaBrokerUrl nn1.p11.prod.saagie.io:9092 --kafkaTopic quotes
* for variations in-memory : spark-submit {driver_options} --class=io.saagie.quotes.QuotesVariationMem --executor-cores 2 --executor-memory 6g --driver-memory 4g --conf "spark.cores.max=4" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+UseCompressedOops" {file} --hdfsNameNodeHost hdfs://nn1.p11.prod.saagie.io:8020 --hdfsPathCheckpoint your-checkpoint-path --hdfsPathQuotes your-data-path --kafkaBrokerUrl nn1.p11.prod.saagie.io:9092 --kafkaTopic quotes

Usage in Saagie :
* sbt clean assembly (in local, to generate jar file)
* create new Spark Job
* upload the jar (target/scala-2.11/quotes.jar)
* choose Spark 2.1.0
* choose resources you want to allocate to the job
* create and launch
