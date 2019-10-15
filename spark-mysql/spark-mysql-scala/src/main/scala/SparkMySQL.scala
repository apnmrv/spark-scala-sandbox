package jdbc

import java.sql.Connection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, _}

object SparkMySQL {
  val jdbcHostname = "localhost"
  val jdbcPort = 33065
  val jdbcDatabase = "sandbox_db"
  val jdbcUsername = "mysql-dev"
  val jdbcPassword = "mysql-pass"

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?autoReconnect=true&useSSL=false&serverTimezone=Europe/Moscow"

  // Create a Properties() object to hold the parameters.
  import java.util.Properties

  val connectionProperties = new Properties()
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")

  import java.sql.DriverManager
  val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

  connection.isClosed()

  var spark: SparkSession = null
  var sc: SparkContext = null
  var startTime: DateTime  = null
  var endTime: DateTime  = null
  val numOfRecordsToCreate = 1000000

  def main(args: Array[String]): Unit = {

    setupSpark

    readData(spark)
  }

  def setupSpark(): Unit = {
    spark = SparkSession
      .builder()
      .appName("Spark-Mysql")
      .master("local[*]")
      .getOrCreate()
  }

  def readData(spark:SparkSession): Unit = {

    val jdbcDF = spark.read
        .jdbc(
          jdbcUrl,
          "crimes",
          connectionProperties
        ).toDF()

    jdbcDF.printSchema()

    jdbcDF.show(false)
  }
}