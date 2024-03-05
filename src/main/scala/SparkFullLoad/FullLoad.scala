package SparkFullLoad


import org.apache.spark.sql.SparkSession



object FullLoad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                .master("local[*]")
                .appName("FullLoad")
                .enableHiveSupport()
                .getOrCreate()


    val df = spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
            .option("dbtable", "car_insurance_claims")
            .option("driver", "org.postgresql.Driver")
            .option("user", "consultants")
            .option("password", "WelcomeItc@2022")
            .load()

    // Get the total count of rows
    val rowCount = df.count()

    // Calculate the row numbers for splitting (80% and 20%)
    val splitPoint = (rowCount * 0.8).toLong

    // Split the data into training and test based on row numbers
    val trainingData = df.limit(splitPoint.toInt)
    val testData = df.except(trainingData)

    // Upload the training data to Hive
    trainingData.write.mode(SaveMode.Overwrite).saveAsTable("training_data")

    // Check if the Hive database exists, and if not, create it
    val hiveDatabase = "Sparky"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDatabase")

    // Save the data to the Hive table in the created database
    spark.sql(s"USE $hiveDatabase")
    trainingData.write.mode(SaveMode.Overwrite).saveAsTable("car_insurance_claims")

    // Perform a full load of the data
    val fullLoadData = spark.table("training_data")

    // Perform further processing or analysis on the full load data if needed

    // Stop SparkSession
    spark.stop()






  }

}
