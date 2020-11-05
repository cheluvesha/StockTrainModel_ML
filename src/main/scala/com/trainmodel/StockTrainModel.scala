package com.trainmodel

import java.io.{File, FileNotFoundException}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object StockTrainModel {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObject("StockTrainModel")
  val pyFilePath =
    "./src/test/PythonFile/StockPrediction.py"
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
  def uploadFile(filePath: String, bucket: String): Int = {
    UtilityClass.uploadPickleFileToS3(filePath, bucket)
  }
  def main(args: Array[String]): Unit = {
    try {
      val bucketName = args(0)
      val fileName = args(1)
      val path: String = "s3a://" + bucketName + "/" + fileName
      val readFileDF: DataFrame = readFileFromS3(path)
      readFileDF.show(false)
      val pipedData = pipePythonFileWithScala(readFileDF, path)
      pipedData.foreach(println(_))
      val status = uploadFile(System.getenv("PICKLE_FILE"), bucketName)
      if (status == 1) {
        println("Uploaded Successfully!!!")
      }
    } catch {
      case arrayIndexOutOfBoundsException: ArrayIndexOutOfBoundsException =>
        println(arrayIndexOutOfBoundsException.printStackTrace())
    }
  }
}
