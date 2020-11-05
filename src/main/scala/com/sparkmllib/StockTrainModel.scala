package com.sparkmllib

import com.sparkmllib.UtilityClass.uploadTrainedModelToS3
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders}

/***
  * Case Class to declare Schema
  * @param Date - DateType
  * @param Open - DoubleType
  * @param High - DoubleType
  * @param Low - DoubleType
  * @param Close - DoubleType
  * @param adjClose - DoubleType
  * @param Volume - DoubleType
  */
case class StockData(
    Date: java.sql.Date,
    Open: Double,
    High: Double,
    Low: Double,
    Close: Double,
    adjClose: Double,
    Volume: Double
)

/***
  * StockTrainModel Reads file from S3 and Trains The Model
  */

object StockTrainModel extends App {

  val sparkSession = UtilityClass.createSparkSessionObject("Stock Train Model")
  val sparkContextObj = sparkSession.sparkContext
  val bucketName = "stockdata-spark-fellowship"
  val fileName = "GOOG.csv"
  val path: String = "s3a://" + bucketName + "/" + fileName
  val stockDataFrame = readFileFromS3(path)
  stockDataFrame.show()
  // DataFrame split
  val Array(trainingData, testData) =
    stockDataFrame.randomSplit(Array(0.8, 0.2))
  // renaming output column name as Label
  val renamedStockDF = stockDataFrame.select(
    col("close").as("label"),
    col("date"),
    col("open"),
    col("high"),
    col("low"),
    col("volume"),
    col("adjClose")
  )

  val assembler = vectorAssembler()
  val output = assembler.transform(renamedStockDF).select("label", "features")
  output.show()
  val model = linearRegressionDetails()
  model.write.overwrite().save(System.getenv("ML_MODEL_PATH"))
  uploadTrainedModelToS3(
    System.getenv("ML_MODEL_DATA")
  )

  /***
    * Reads File from S3 and Creates DataFrame
    * @param filePath - S3 file path
    * @return DataFrame
    */
  def readFileFromS3(filePath: String): DataFrame = {
    try {
      Configuration.apply(sparkContextObj).hadoopAwsConfiguration()
      val csvDataDF = sparkSession.read
        .option("header", value = true)
        .schema(Encoders.product[StockData].schema)
        .option("dateFormat", "yyyy-mm-dd")
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
  // creates column with required input and output fields
  def vectorAssembler(): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(
        Array(
          "open",
          "high",
          "low",
          "volume"
        )
      )
      .setOutputCol("features")
  }
  // Linear Regression Model creation
  def linearRegressionDetails(): LinearRegressionModel = {
    val lr = new LinearRegression()
    val lrModel = lr.fit(output)
    val trainingSummary = lrModel.summary
    trainingSummary.residuals.show()
    println("R^2: " + trainingSummary.r2)
    println("Root Mean Sq Value is :" + trainingSummary.rootMeanSquaredError)
    trainingSummary.predictions.show()
    lrModel
  }

}
