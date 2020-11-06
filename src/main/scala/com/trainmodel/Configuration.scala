package com.trainmodel

import org.apache.spark.SparkContext

/***
  * Case class which configures Aws S3 as FileSystem with Key and ID
  * Dependencies included s3a
  * @param sparkContextObj SparkContext Type
  */
case class Configuration(
    sparkContextObj: SparkContext,
    awsConfiguration: AWSConfiguration
) {
  val awsConfig: AWSConfiguration = awsConfiguration
  val awsAccessKeyID: String = System.getenv("AWS_ACCESS_KEY_ID")
  val awsSecretAccessKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")

  /***
    * Configures Aws S3 with Credentials
    */
  def hadoopAwsConfiguration(): Int = {
    awsConfig.connectToS3(awsAccessKeyID, awsSecretAccessKey, sparkContextObj)
  }
}
