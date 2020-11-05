name := "MLWithSpark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.890"

libraryDependencies += "com.github.seratch" %% "awscala-s3" % "0.8.+"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll (
  ExclusionRule("com.amazonaws", "aws-java-sdk"),
  ExclusionRule("commons-beanutils")
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
