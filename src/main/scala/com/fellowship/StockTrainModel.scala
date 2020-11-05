package com.fellowship

import com.fellowship.UtilityClass.uploadPickleFileToS3
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Reads Data from Aws S3
  * Trains Model by using ML algorithm python file and Passes pickle file to upload it to S3
  */
object StockTrainModel extends App {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObject("Stock Train Model")
  val sparkContextObj: SparkContext = sparkSession.sparkContext

  /***
    * Reads Data From Aws S3 and Creates DataFrame
    * @param filePath file path which is stored in Aws S3
    * @return DataFrame
    */
  def readFileFromS3(filePath: String): DataFrame = {
    try {
      Configuration(sparkContextObj).hadoopAwsConfiguration()
      val csvDataDF = sparkSession.read
        .option("header", value = true)
        .csv(filePath)
      csvDataDF
    } catch {
      case ioException: java.io.IOException =>
        println(ioException.printStackTrace())
        sparkSession.emptyDataFrame
      case exception: Exception =>
        println(exception.printStackTrace())
        sparkSession.emptyDataFrame
    }
  }

  try {
    val bucketName = "stockdata-spark-fellowship"
    val fileName = "GOOG.csv"
    val path: String = "s3a://" + bucketName + "/" + fileName
    val readFileDF = readFileFromS3(path)
    readFileDF.printSchema()
    readFileDF.show(false)
    val pyFilePath =
      "./src/test/PythonFile/StockPrediction.py"
    val pathToBePiped =
      "python3 " + pyFilePath
    val pipeData = readFileDF.rdd.pipe(pathToBePiped)
    pipeData.foreach(println(_))

    /***
      * Calls Upload method to upload pickle file into Aws S3
      */
    uploadPickleFileToS3(System.getenv("PICKLE_FILE"))
  } catch {
    case arrayIndexOutOfBoundsException: ArrayIndexOutOfBoundsException =>
      println("Please pass only required arguments")
    case exception: Exception =>
      println(exception.printStackTrace())
  }
}
