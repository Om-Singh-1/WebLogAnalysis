package com.weblog.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object LogAnalyzer {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")        // Running this code on local computer [*] means use all cores to distribute tasks.
      .getOrCreate()

    val dsOneSchema = StructType(
      StructField("ip_address", StringType, true) ::
      StructField("date", StringType, true) ::
      StructField("http_get", StringType, true) ::
      StructField("http_status", StringType, true) ::
      StructField("rel_link", StringType, true) :: Nil)

    val dsTwoSchema = StructType (
      StructField("ip_address", StringType, true) ::
        StructField("country", StringType, true) ::
        StructField("browser", StringType, true) ::
        StructField("city", StringType, true) :: Nil)

    // Reading and applying schema to both the csv files.
    val dsOne = spark.read.option("header", "true").schema(dsOneSchema).csv("data/weblogs.csv")

    val dsTwo = spark.read.option("header", "true").schema(dsTwoSchema).csv("data/IP_Mapping.csv")

    //Joining the dataframe.
    val united_df = dsOne.join(dsTwo, "ip_address" )

    // Filtering by session and then converting the dataframe to rdd for easy counting.
    val question_one = united_df.select(united_df.col("country")).filter("http_status == 200").rdd.map(x => (x,1)).reduceByKey( (x, y) => x + y)
    println("---------------Session By Country----------------")
    question_one.foreach(println)

    // Filtering by session and city and then counting
    val question_two = united_df.select(united_df.col("city")).filter("http_status == 200").rdd.map( x => (x, 1)).reduceByKey( (x, y) => (x + y))
    println("---------------Session By City-------------------")
    question_two.foreach(println)

    // Filtering by session and browser type and then counting
    val question_three = united_df.select(united_df.col("browser")).filter("http_status == 200").rdd.map(x => (x, 1)).reduceByKey((x, y) => (x + y))
    println("---------------Session By Browser Type-------------------")
    question_three.foreach(println)

    val question_four = united_df.select(united_df.col("http_get")).where("http_status != 200").limit(1000).rdd.map((x => (x,1))).reduceByKey( (x,y ) => (x + y))
    println("---------------Errors Per Thousand Requests Per Page-------------------")
    question_four.foreach(println)

    import spark.implicits._

    // Above code was just to show the output below the rdd is converted to Dataframe and saved to a file
    // Saving to CSV file
    val x_of_questionOne = question_one.map( x => (x._1.mkString("["), x._2) ).toDF("Country", "Count")
    x_of_questionOne.toDF().coalesce(1).orderBy("Count").write.mode(SaveMode.Overwrite).option("header", "true").csv("data/questionOne")

    val x_of_questionTwo = question_two.map(x => (x._1.mkString("["), x._2)).toDF("City", "Count")
    x_of_questionTwo.toDF().coalesce(1).orderBy("Count").write.mode(SaveMode.Overwrite).option("header", "true").csv("data/questionTwo")

    val x_of_questionThree = question_three.map(x => (x._1.mkString("["), x._2)).toDF("Browser Type", "Count")
    x_of_questionThree.toDF().coalesce(1).orderBy("Count").write.mode(SaveMode.Overwrite).option("header", "true").csv("data/questionThree")

    val x_of_questionFour = question_four.map(x => (x._1.mkString("["), x._2)).toDF("No of errors per thousand requests", "Count")
    x_of_questionFour.toDF().coalesce(1).orderBy("Count").write.mode(SaveMode.Overwrite).option("header", "true").csv("data/questionFour")


    // Testing replace function.
    // val x_of_questionTest = question_four.map(x => (x._1.toString().replace("[", ""), x._2)).toDF("No of errors per thousand requests", "Count").show()

  }
}