package org.example.shivam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, monotonically_increasing_id, regexp_replace}

object testingCode {

  case class DateDimension(date: String)

  case class StateDimension(state: String)

  case class TestingFact(date: String, state: String, total_samples: Int, positive: Int, negative: Int)

  val path_to_input_file = "C:\\Users\\shiva\\Documents\\DataEng\\testing_ip.csv"
  val path_to_output_directory = "C:\\Users\\shiva\\Documents\\DataEng\\asses3\\outputs\\"

  def process_data(spark: SparkSession): Unit = {

    // Read the CSV file
    val rawData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path_to_input_file)

    // Create the date and state dimensions
 rawData.show()
//!col("col2").isin("ty") or

    val ty=rawData.withColumn("col4",coalesce(col("col2"),lit("unknown"))).filter(!col("col4").isin("ty"))
      .drop(col("col4"))

    ty.show()

  }


  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\software\\winutils\\hadoop-3.3.0" //"C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }


    val spark = SparkSession.builder
      .appName("Simple Stream Application")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")

    process_data(spark)

  }


}
