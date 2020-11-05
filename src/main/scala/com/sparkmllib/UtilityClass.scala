package com.sparkmllib

import java.io.FileNotFoundException

import awscala.s3.{Bucket, S3}
import com.fellowship.UtilityClass.s3.bucket
import org.apache.spark.sql.SparkSession
import awscala._

/***
  * Utility class Which creates object and performs other functions
  */
object UtilityClass {
  val bucketName = "stock-trained-model-scala"
  implicit val s3: S3 = S3.at(Region.US_WEST_1)

  /***
    * Creates Spark Session Object
    * @param name App name
    * @return SparkSession Type
    */
  def createSparkSessionObject(name: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name)
      .master("local[*]")
      .getOrCreate()
    spark
  }

  /***
    * Checks the File Path Valid or Not
    * @param filePath  String
    * @return Boolean
    */
  def checkFilePathValidOrNot(filePath: String): Boolean = {
    val file = new File(filePath)
    file.exists()
  }

  /***
    * Checks Bucket Exists in S3
    * @param bucket Bucket
    * @return Boolean
    */
  def checkBucketExistsOrNot(bucket: String): Boolean =
    s3.doesBucketExistV2(bucket)

  /***
    * Creates Bucket in AWS S3
    * @return Bucket - Bucket Which Created
    */
  def createBucket(): Bucket = s3.createBucket(bucketName)

  /***
    * Extract the file from Directory
    * @param directoryPath - String
    * @return String
    */
  def extractFile(directoryPath: String): String = {
    val directory = new File(directoryPath)
    val dirList = directory.listFiles()
    var parquetFile = ""
    for (file <- dirList) {
      if (file.getName.endsWith(".parquet")) {
        parquetFile = file.getAbsolutePath
      }
    }
    parquetFile
  }

  /***
    * Uploads Pickle file to AWS S3
    * @param filePath path where pickle file is stored
    */
  def uploadTrainedModelToS3(filePath: String): Unit = {
    try {
      val path = extractFile(filePath)
      val fileCheck: Boolean = checkFilePathValidOrNot(path)
      if (fileCheck) {
        val status = checkBucketExistsOrNot(bucketName)
        if (status) {
          val bucketToPut: Bucket = bucket(bucketName).last
          s3.putObject(
            bucketToPut,
            "StockPriceModelScala.parquet",
            new java.io.File(path)
          )
          println("Successfully file uploaded to Existing Bucket in S3 ")
        } else {
          val bucket = createBucket()
          bucket.put("StockPriceModelScala.parquet", new java.io.File(path))
          println("Successfully file uploaded to New Bucket in S3 ")
        }
      } else {
        throw new FileNotFoundException()
      }
    } catch {
      case fileNotFoundException: FileNotFoundException =>
        println(fileNotFoundException.printStackTrace())
    }
  }
}
