package com.trainmodel

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatestplus.mockito.MockitoSugar

/***
  * StockTrainModelTest - Test the functionalities of StockTrainModel class
  * Dependencies Used ScalaTest and Mockito
  */
class StockTrainModelTest
    extends FunSuite
    with BeforeAndAfter
    with MockitoSugar {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[8]")
    .appName("Test app")
    .getOrCreate()
  val csvFilePath = "./src/test/Resources/GOOG.csv"
  val s3URL = "s3a://stockdata-spark-fellowship/GOOG.csv"
  val wrongCSV = "./src/test/Resources/Housing.csv"
  val wrongPath = "a.csv"
  val pythonFile = "./src/test/PythonFile/StockPrediction.py"
  val stockTrainModel: StockTrainModel.type = StockTrainModel
  val csvDataDF: DataFrame = sparkSession.read
    .option("header", value = true)
    .csv(csvFilePath)

  test("givenCSVShouldReadAndValidateWithMainClassMethod") {
    val stockDF: DataFrame =
      stockTrainModel.readFileFromS3(s3URL)
    val dataFrameCompare = new DataFrameComparison
    assert(dataFrameCompare.compareDataFrames(csvDataDF, stockDF) === true)
  }

  test("givenWrongCSVShouldReturnFalseWhenCompared") {
    val wrongCSVDF = sparkSession.read
      .option("header", value = true)
      .csv(wrongCSV)
    val stockDF: DataFrame =
      stockTrainModel.readFileFromS3(s3URL)
    val dataFrameCompare = new DataFrameComparison
    assert(dataFrameCompare.compareDataFrames(wrongCSVDF, stockDF) === false)
  }

  test("givenWrongPathShouldThrownAnExceptionReadFunction") {
    val thrown = intercept[Exception] {
      stockTrainModel.readFileFromS3(wrongPath)
    }
    assert(thrown.getMessage === "Please Check the Path")
  }

  test("givenPathToPerformPipeShouldEqualToTrueWhenDataComparedIsEqual") {
    val pipeDataFile = "python3 " + pythonFile
    val pipedData = csvDataDF.rdd.pipe(pipeDataFile).collect()
    val resultRDD =
      stockTrainModel.pipePythonFileWithScala(csvDataDF, pythonFile).collect()
    assert(pipedData === resultRDD)
  }

  test("givenPathToPerformPipeShouldNotEqualToExpected") {
    val pipeDataFile = "python3 " + pythonFile
    val pipedData = csvDataDF.rdd.pipe(pipeDataFile)
    val resultRDD =
      stockTrainModel.pipePythonFileWithScala(csvDataDF, pythonFile)
    assert(pipedData != resultRDD)
  }

  test("givenWrongPathToPipeShouldThrowAnExceptionPipeFunction") {
    val thrown = intercept[Exception] {
      stockTrainModel.pipePythonFileWithScala(csvDataDF, wrongPath)
    }
    assert(thrown.getMessage === "Please Verify the Path and Pass Valid one")
  }

  test("givenFilePathToUploadItToS3WhenSuccessFullShouldReturn1") {
    val resultData = stockTrainModel.uploadFile(pythonFile, "test-stock-bucket")
    assert(resultData === 1)
  }

  //test("givenFilePathToUploadWhenNotSuccessFulShouldReturn-1") {
  //val resultData = stockTrainModel.uploadFile(pythonFile, "test-stock-bucket")
  // assert(resultData === -1)
  //}

  test("givenWrongFilePathShouldThrownAnExceptionInUploadFunction") {
    val thrown = intercept[Exception] {
      stockTrainModel.uploadFile(wrongPath, "test-bucket-stock")
    }
    assert(thrown.getMessage === "Please Check the Path, Upload is Failed")
  }

  test("givenConfigureCredentialsShouldVerifyByMocking") {
    val service = mock[Configure]
    when(
      service.hadoopAwsConfiguration(
        System.getenv("AWS_ACCESS_KEY_ID"),
        System.getenv("AWS_SECRET_ACCESS_KEY")
      )
    ).thenReturn(1)
    when(
      service.hadoopAwsConfiguration(
        "123456",
        "658"
      )
    ).thenReturn(-1)

    val realCred = service.hadoopAwsConfiguration(
      System.getenv("AWS_ACCESS_KEY_ID"),
      System.getenv("AWS_SECRET_ACCESS_KEY")
    )
    val fakeCred = service.hadoopAwsConfiguration(
      "123456",
      "658"
    )

    // (4) verify the results
    assert(realCred === 1)
    assert(fakeCred === -1)
  }
}
