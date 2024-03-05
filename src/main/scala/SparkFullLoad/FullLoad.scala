package SparkFullLoad


import org.apache.spark.sql.SparkSession



object FullLoad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                .master("local[*]")
                .appName("FullLoad")
                //.enableHiveSupport()
                .getOrCreate()


    val df = spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
            .option("dbtable", "car_insurance_claims")
            .option("driver", "org.postgresql.Driver")
            .option("user", "consultants")
            .option("password", "WelcomeItc@2022")
            .load()

    // Count total number of rows
    val totalRows = df.count()

    // Calculate row numbers for 80% and 20% splits
    val split80Percent = (totalRows * 0.8).toLong
    val split20Percent = totalRows - split80Percent


    println(df.printSchema())
    println(df.show(10))
    println("Automated")
  }

}
