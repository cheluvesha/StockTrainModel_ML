package com.trainmodel

import java.io.{File, FileNotFoundException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * StockTrainModel - Trains Model and Calls method to Upload it to S3
  * Dependencies included Spark core and Spark sql
  * plugin used SBT-Assembly
  */

object StockTrainModel {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObject("StockTrainModel")

  /***
    * Reads CSV file and Creates DataFrame
    * @param filePath String
    * @return DataFrame
    */
  def readFileFromS3(filePath: String): DataFrame = {
    try {
      Configuration(sparkSession.sparkContext).hadoopAwsConfiguration()
      val csvDataDF = sparkSession.read
        .option("header", value = true)
        .csv(filePath)
      csvDataDF
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        throw new FileNotFoundException("Please Check the Path")
    }
  }

  /***
    * Performs Pipe Operation with Python file
    * @param dataFrame DataFrame
    * @param filePath  String
    * @return RDD[String]
    */
  def pipePythonFileWithScala(
      dataFrame: DataFrame,
      filePath: String
  ): RDD[String] = {
    val file = new File(filePath)
    if (!file.exists()) {
      throw new FileNotFoundException(
        "Please Verify the Path and Pass Valid one"
      )
    }
    val pathToBePiped =
      "python3 " + filePath
    val pipeData = dataFrame.rdd.pipe(pathToBePiped)
    pipeData
  }

  /***
    * Passes Parameters to upload pkl file to S3
    * @param filePath String
    * @param bucket String Bucket name
    * @return Int
    */
  def uploadFile(filePath: String, bucket: String): Int = {
    UtilityClass.uploadPickleFileToS3(filePath, bucket)
  }

  /***
    * Entry point to train model
    * @param args Array[String]
    */
  def main(args: Array[String]): Unit = {
    try {
      val bucketName = "stockdata-spark-fellowship"
      val fileName = "GOOG.csv"
      val path: String = "s3a://" + bucketName + "/" + fileName
      val bucketToStorePKL = "stock-trained-model"
      val readFileDF: DataFrame = readFileFromS3(path)
      readFileDF.show(false)
      val pipedData = pipePythonFileWithScala(
        readFileDF,
        System.getenv("PY_FILE")
      )
      pipedData.foreach(println(_))
      val status =
        uploadFile(System.getenv("PKL_FILE"), bucketToStorePKL)
      if (status == 1) {
        println("All Task Done And Model Uploaded Successfully!!!")
      }
    } catch {
      case arrayIndexOutOfBoundsException: ArrayIndexOutOfBoundsException =>
        println(arrayIndexOutOfBoundsException.printStackTrace())
    }
  }
}
