package SparkFullLoad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object IncrLoad {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("IncrLoad")
      .enableHiveSupport()
      .getOrCreate()

    val maxIdDF = spark.sql("SELECT max(id) FROM sparky.car_insurance_claims")
    val maxId = maxIdDF.head().getInt(0).toLong
    println(maxId)

    val query = s"""SELECT * FROM emp_info_scala WHERE "ID" > $maxId"""

    val moreData = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("driver", "org.postgresql.Driver")
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022")
      .option("query", query)
      .load()

    println(moreData.printSchema())
    println(moreData.show(10))
    println("Automated")

  }
}
