package io.saagie.quotes

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
    spark.readStream
      ???
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
      ???

    // Compute evolution
    val meanPrice = dataset.withColumn("timestamp", $"ts".cast("timestamp"))
      ???

    // Start running the query and write the result into a file
    writeStream(meanPrice)
  }
}
