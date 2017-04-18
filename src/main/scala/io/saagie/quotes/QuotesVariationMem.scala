package io.saagie.quotes

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQueryListener}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Computes quotes variation.
  */
object QuotesVariationMem extends SparkLauncher {
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
    // Add listeners to do batches over in-memory data
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        import spark.sql
        val query = sql("select ts, label," +
          "cast(((price - qp)/qp) * 100 as float) as value from " +
          "(select price as qp, label as ql, ts as qt from quotes inner join " +
          "(select min(ts) as tt, label as tl from quotes group by label)" +
          "on tt = ts and tl = label), quotes where label = ql " +
          "order by ts desc, label")
        query.write.mode("append").parquet(params.hdfsNameNodeHost + params.hdfsPathQuotes)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    dataset.writeStream
      .trigger(ProcessingTime(params.triggerTime))
      .format("memory")
      .queryName("quotes")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
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

    // Start running the query and write the result into a file
    writeStream(dataset.withColumn("label", $"symbol".as('label)))
  }
}
