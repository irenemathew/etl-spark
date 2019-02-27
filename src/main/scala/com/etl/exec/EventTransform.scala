package com.etl.exec

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object EventTransform {
  val logger = Logger.getLogger("EventTransform")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL ETL App")
      .config("spark.driver.host", "localhost")
      .enableHiveSupport()
      .getOrCreate()

    loadCleansedData(spark,args(0))
    logger.info("Cleansed JSON load completed")

    val userTableLoc = args(1) + "/user_details/"
    userTransform(spark,userTableLoc)
    logger.info("user_details transform completed")

    val activityTableLoc = args(1) + "/activity_details/"
    activityTransform(spark,activityTableLoc)
    logger.info("activity_details transform completed")

    val tradeTableLoc = args(1) + "/trade_demand/"
    findPopularTrade(spark,tradeTableLoc)
    logger.info("Frequently searched trade transform completed")
    spark.stop()
  }
  /** Reads JSON data from input file, cleanses and transforms data and saves it as a temporary table
    *
    * @param spark Spark Session created
    * @param path path of input file
    *
    *
    */
  private def loadCleansedData(spark: SparkSession,path: String): Unit = {
    val eventDF = spark.read.json(path)
    eventDF.createOrReplaceTempView("event")
    val cleansedDF = spark.sql(loadResource("/sql/cleansing.sql"))
    cleansedDF.createOrReplaceTempView("event_master")
  }

  /** Performs transformation at user level and saves the result as external hive table in CSV format
    *
    * @param spark Spark Session created
    * @param userTableLoc path of external hive table
    *
    */

  private def userTransform(spark: SparkSession, userTableLoc: String): Unit = {
    val usersDF = spark.sql(loadResource("/sql/user_transform.sql"))
    usersDF.write
      .format("csv")
      .option("nullValue", "NULL")
      .save(userTableLoc)
    usersDF.createOrReplaceTempView("users")
    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS user_details " +
      "(user_id string, time_stamp string, " +
      "url_level1 string, url_level2 string, " +
      "url_level3 string, activity string ) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY ',' " +
      s"LOCATION '$userTableLoc' ")
    spark.sql("SELECT * FROM user_details").show()  //demo purpose
  }

  /** Performs transformation at hourly level and saves the result as external hive table in CSV format
    *
    * @param spark Spark Session created
    * @param activityTableLoc path of external hive table
    *
    */

  private def activityTransform(spark: SparkSession, activityTableLoc: String): Unit = {
    val activityDF=spark.sql(loadResource("/sql/activity_transform.sql"))
    activityDF.coalesce(1).write
      .format("csv")
      .option("nullValue", "NULL")
      .save(activityTableLoc)
    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS activity_details " +
      "(time_bucket string, url_level1 string, " +
      "url_level2 string, activity string, " +
      "activity_count string, user_count string ) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY ',' " +
      s"LOCATION '$activityTableLoc' ")
    spark.sql("SELECT * FROM activity_details").show()  //demo purpose
  }

  /** Performs frequently searched trade in a month and saves the result as external hive table in CSV format
    *
    * @param spark Spark Session created
    * @param tradeTableLoc path of external hive table
    *
    */

  private def findPopularTrade(spark: SparkSession, tradeTableLoc: String): Unit = {
    val tradeDF=spark.sql(loadResource("/sql/trade_demand_transform.sql"))
    tradeDF.coalesce(1).write
      .format("csv")
      .option("nullValue", "NULL")
      .save(tradeTableLoc)
    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS trade_demand " +
      "(month_bucket string, trade string, search_frequency string ) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY ',' " +
      s"LOCATION '$tradeTableLoc' ")
    spark.sql("SELECT * FROM trade_demand").show() //demo purpose
  }
  /** Reads file in the resource path and returns the content
    *
    * @param filename name of the file
    * @return String content of resource path file
    */
  private def loadResource(filename: String) = {
    val source = scala.io.Source.fromURL(getClass.getResource(filename))
    try source.mkString finally source.close()
  }

}
