package com.trainmodel

import java.io.FileNotFoundException

import awscala.s3.{Bucket, S3}
import com.fellowship.UtilityClass.s3.bucket
import org.apache.spark.sql.SparkSession
import awscala._
import com.amazonaws.services.s3.model.AmazonS3Exception

object UtilityClass {

  var bucketName: String = _
  implicit val s3: S3 = S3.at(Region.US_WEST_1)
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
    * Uploads Pickle file to AWS S3
    *By validating path and aws s3 bucket
    * @param filePath String - path where pickle file is stored
    */
  def uploadPickleFileToS3(filePath: String, bucketToUpload: String): Int = {
    try {
      bucketName = bucketToUpload
      val fileCheck: Boolean = checkFilePathValidOrNot(filePath)
      if (fileCheck) {
        val status = checkBucketExistsOrNot(bucketName)
        if (status) {
          val bucketToPut: Bucket = bucket(bucketName).last
          s3.putObject(
            bucketToPut,
            "StockPriceModel.pkl",
            new java.io.File(filePath)
          )
          println("Successfully file uploaded to Existing bucket in S3 ")
        } else {
          val bucket = createBucket()
          bucket.put("StockPriceModel.pkl", new java.io.File(filePath))
          println("Successfully file uploaded to New Bucket in S3 ")

        }
        1
      } else {
        throw new FileNotFoundException("Please check the File Path")
      }
    } catch {
      case fileNotFoundException: FileNotFoundException =>
        throw new FileNotFoundException(
          "Please Check the Path, Upload is Failed"
        )
      case amazonS3Exception: com.amazonaws.SdkClientException =>
        println(amazonS3Exception.printStackTrace())
        -1
    }
  }
}
