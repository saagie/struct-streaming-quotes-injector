package io.saagie.quotes

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * Abstract launcher to simplify Spark and Kafka's use.
  */
abstract class SparkLauncher {

  /**
    * Parameters for this app.
    */
  protected case class CLIParams(
                                  triggerTime: Long = 0L,
                                  hdfsNameNodeHost: String = "",
                                  hdfsPathCheckpoint: String = "",
                                  hdfsPathQuotes: String = "",
                                  kafkaBrokerUrl: String = "",
                                  kafkaTopic: String = ""
                                )

  // CONSTANTS
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val appName: String = this.getClass.getName

  // ABSTRACT FIELDS
  protected val sparkConf: SparkConf = new SparkConf().setAppName(appName).setMaster(System.getProperty("spark.master"))
  protected var params: CLIParams = CLIParams()

  /**
    * Method where your ETL goes.
    */
  protected def compute(): Unit

  // MAIN METHOD
  /**
    * Entry point.
    */
  def main(args: Array[String]): Unit = {
    val parser = parseArgs(appName)
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    parser.parse(args, CLIParams()) match {
      case Some(cliParams) =>
        logger.info("Trigger time : %sms.".format(cliParams.triggerTime))
        params = cliParams
        compute()
      case None => logger.debug("invalid program arguments : {}", args)
    }
  }

  // TOOLS
  /**
    * Parse arguments from command line.
    *
    * @param appName application name.
    * @return parsed arguments.
    */
  private def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      // Trigger configuration
      opt[Long]("triggerTime") action { (data, conf) =>
        if (data >= 0) conf.copy(triggerTime = data) else conf
      } text "Time between batches. Default 0ms"

      // HDFS configuration
      opt[String]("hdfsNameNodeHost") required() action { (data, conf) =>
        conf.copy(hdfsNameNodeHost = data)
      } text "URI of hdfs nameNode. Example : hdfs://IP:8020"

      opt[String]("hdfsPathCheckpoint") required() action { (data, conf) =>
        conf.copy(hdfsPathCheckpoint = data)
      } text "Directory path where the checkpoiting data are writen. Example: /user/spark/checkpointing/injector "

      opt[String]("hdfsPathQuotes") required() action { (data, conf) =>
        conf.copy(hdfsPathQuotes = data)
      } text "Directory path where the quotes data are writen : Example: /user/hdfs/quotes"

      // Kafka configuration
      opt[String]("kafkaBrokerUrl") required() action { (data, conf) =>
        conf.copy(kafkaBrokerUrl = data)
      } text "Url of kafka broker : Example: localhost:9092"

      opt[String]("kafkaTopic") required() action { (data, conf) =>
        conf.copy(kafkaTopic = data)
      } text "Name of your quote topics"
    }
  }
}