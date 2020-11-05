package com.LinearRegressionDemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * Linear Regression to Predict the price
  */
object LinearRegression extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("LinearRegression")
    .getOrCreate()

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  // reads CSV and converts it into DataFrame
  val inputDF = spark.read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("./src/test/Resources/Housing.csv")
  inputDF.printSchema()
  inputDF.show()

  val df = inputDF.select(
    col("Price").as("label"),
    col("Avg Area Income"),
    col("Avg Area House Age"),
    col("Avg Area Number of Rooms"),
    col("Avg Area Number of Bedrooms"),
    col("Area Population")
  )
  // converts multiple columns to single vector column
  val assembler = new VectorAssembler()
    .setInputCols(
      Array(
        "Avg Area Income",
        "Avg Area House Age",
        "Avg Area Number of Rooms",
        "Avg Area Number of Bedrooms",
        "Area Population"
      )
    )
    .setOutputCol("features")

  val output = assembler.transform(df).select("label", "features")
  output.show()
  val lr = new LinearRegression()
  val lrModel = lr.fit(output)
  val trainingSummary = lrModel.summary
  trainingSummary.residuals.show()
  println(trainingSummary.rootMeanSquaredError)
  println(trainingSummary.predictions.show())

}
