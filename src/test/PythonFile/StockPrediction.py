#!/usr/bin/python`
"""
The objective is to take input from a Amazon S3 resource and call machine learning python file
and save the trained model to S3 using Spark Scala.
Library Used -
1> pandas [For Creating DataFrames]
  Version - 1.1.2
2> pickle [For Dumping Model]
  Version - 3.8
3> Sklearn [For Machine Learning Algorithms]
  Version - 0.23.2
"""


import sys
import numpy as np
import pandas as pd
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import sklearn.metrics as metrics
import os

"""
Function to Upload File to S3
@:param LinearRegressionModel
"""
def saveFileToPath(lrModel):
    try:
        with open("./src/test/Resources/StockPriceModel.pkl", "wb") as file:
            pickle.dump(lrModel, file)
    except FileNotFoundError:
        print("The file was not found")



#Taking input from Spark's RDD through Pipe
inputFile = sys.stdin
#Creating a DataFrame from the inputFile
inputStockPriceDF = pd.read_csv(inputFile, header=None)
#Assigning Column Names
inputStockPriceDF.columns= ["Date","Open","High","Low","Close","Adj Close","Volume"]
inputStockPriceDF["Volume"] = inputStockPriceDF["Volume"].apply(lambda x : x.replace("]",""))
#Taking Only the Needed Input Columns
inputCols = inputStockPriceDF.drop(["Close","Adj Close","Date"],axis = 1)
#Separating Output Column
outputCol = inputStockPriceDF["Close"]
#Creating a LinearRegression Model
lrModel = LinearRegression()
#Splitting the data in for one training our model and other to test
X_train,X_test,y_train,y_test = train_test_split(inputCols,outputCol,test_size = 0.2,random_state = 101)
#Fitting our model on our training set
lrModel.fit(X_train,y_train)
#Printing the R^2 [Goodness of Fit] for our test data
print("The R^2 value is ",lrModel.score(X_test,y_test))
#Predicting the result for our test data
y_pred = lrModel.predict(X_test)
#Calculating and printing Root Mean Square Error and Error Value
rmse = np.sqrt(metrics.mean_squared_error(y_test,y_pred))
print("The root mean square error is {:.2f}".format(rmse))
avgValue = np.mean(y_test)
print("The error Value is {:.2f} %".format(rmse / avgValue ))
#Calling function to upload model to S3
saveFileToPath(lrModel)









