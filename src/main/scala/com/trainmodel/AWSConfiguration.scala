package com.trainmodel

import org.apache.spark.SparkContext

class AWSConfiguration() {
  def connectToS3(
      awsAccessKeyID: String,
      awsSecretAccessKey: String,
      sparkContextObj: SparkContext
  ): Int = {
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
    1
  }

}
