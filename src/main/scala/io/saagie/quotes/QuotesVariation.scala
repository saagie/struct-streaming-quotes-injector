package io.saagie.quotes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Computes quotes average over the last 2 minutes.
  */
object QuotesVariation extends SparkLauncher {
  /**
    * Spark's session, used for Structured Streaming / SparkSQL.
    */
  private lazy val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  /**
    * Read on a Kafka stream.
    *
    * @return The stream's DataFrame.
    */
  private def readStream(): DataFrame = {
    spark
      .readStream
      .format("kafka") // From Kafka
      .option("kafka.bootstrap.servers", params.kafkaBrokerUrl) // Broker URL
      .option("subscribe", params.kafkaTopic) // Topic name
      .load()
  }

  /**
    * Writes in a HDFS directory.
    *
    * @param dataset the dataset to write.
    */
  private def writeStream[T](dataset: Dataset[T]): Unit = {
    val query = dataset.writeStream
      .trigger(ProcessingTime(params.triggerTime))
      .format("parquet")
      .outputMode(OutputMode.Append())
      .option("path", params.hdfsNameNodeHost + params.hdfsPathQuotes)
      .option("checkpointLocation", params.hdfsPathCheckpoint)
      .start()
    query.awaitTermination()
  }

  /**
    * Read from Kafka stream, query on it, and write in HDFS file.
    */
  override def compute(): Unit = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    // Create Dataset
    val dataset = readStream().selectExpr("CAST(value AS STRING)")
      .as[String]
      .flatMap(Quote.parse(_) match {
        case Some(q) => Some(q)
        case _ => None
      })

    // Compute evolution
    val meanPrice = dataset.withColumn("timestamp", $"ts".cast("timestamp"))
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp", "2 minutes") as 'time, $"symbol" as 'label)
      .agg(avg("price").as('avgPrice))

    // Start running the query and write the result into a file
    writeStream(meanPrice)
  }
}
