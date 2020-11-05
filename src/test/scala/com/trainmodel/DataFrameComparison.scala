package com.trainmodel

import org.apache.spark.sql.DataFrame

/***
  * Class which compares Two DataFrames for Testing Purpose
  * DataFrame type data structure declared
  */
class DataFrameComparison {

  // function compares Data between two DataFrames
  def compareDataFrames(
      actualDataFrame: DataFrame,
      expectedDataFrame: DataFrame
  ): Boolean = {
    if (
      !actualDataFrame.schema
        .toString()
        .equalsIgnoreCase(expectedDataFrame.schema.toString())
    ) {
      return false
    }
    if (
      actualDataFrame
        .unionAll(expectedDataFrame)
        .except(actualDataFrame.intersect(expectedDataFrame))
        .count() != 0
    ) {
      return false
    }
    true
  }
}
