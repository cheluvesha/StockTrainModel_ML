package com.trainmodel

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class StockTrainModelTest extends FunSuite {
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
    val csvDataDF = sparkSession.read
      .option("header", value = true)
      .csv(wrongCSV)
    val stockDF: DataFrame =
      stockTrainModel.readFileFromS3(s3URL)
    val dataFrameCompare = new DataFrameComparison
    assert(dataFrameCompare.compareDataFrames(csvDataDF, stockDF) === false)
  }
  test("givenWrongPathShouldThrownAnException") {
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
  test("givenWrongPathToPipeShouldThrowAnException") {
    val thrown = intercept[Exception] {
      stockTrainModel.pipePythonFileWithScala(csvDataDF, wrongPath)
    }
    assert(thrown.getMessage === "Please Verify the Path and Pass Valid one")
  }
  test("givenFilePathToUploadItToS3WhenSuccessFullShouldReturn1") {
    val resultData = stockTrainModel.uploadFile(pythonFile, "test-stock-bucket")
    assert(resultData === 1)
  }
  test("givenFilePathToUploadWhenNotSuccessFulShouldReturn-1") {
    val resultData = stockTrainModel.uploadFile(pythonFile, "test-stock-bucket")
    assert(resultData === -1)
  }

}
