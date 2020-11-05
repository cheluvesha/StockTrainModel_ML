package com.trainmodel

import org.apache.spark.SparkContext

/***
  * Case class which configures Aws S3 as FileSystem with Key and ID
  * Dependencies included s3a
  * @param sparkContextObj SparkContext Type
  */
case class Configuration(sparkContextObj: SparkContext) {
  val awsAccessKeyID: String = System.getenv("AWS_ACCESS_KEY_ID")
  val awsSecretAccessKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")

  /***
    * Configures Aws S3 with Credentials
    */
  def hadoopAwsConfiguration(): Unit = {
    System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2")
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsAccessKeyId", awsAccessKeyID)
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsSecretAccessKey", awsSecretAccessKey)
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.endpoint", "s3.amazonaws.com")
  }

}
