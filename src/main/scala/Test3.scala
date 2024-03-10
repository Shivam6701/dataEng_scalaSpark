
package org.example.shivam
import org.apache.spark.sql.functions.{avg, col, expr, mean, monotonically_increasing_id, row_number, sec, to_date, window}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}



object Test3 {
  case class DateDimension(date: String)
  case class StateDimension(state: String)
  case class TestingFact(date: String, state: String, total_samples: Int, positive: Int, negative: Int)
  val path_to_input_file="C:\\Users\\shiva\\Documents\\DataEng\\asses3\\inputs\\StatewiseTestingDetails.csv"
  val path_to_output_directory="C:\\Users\\shiva\\Documents\\DataEng\\asses3\\outputs\\"

  def process_data(spark: SparkSession): Unit= {

    // Read the CSV file
    val rawData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path_to_input_file)

    // Create the date and state dimensions
    val dateDimension = rawData.select(col("Date")).distinct()
    val dimDate=dateDimension.withColumn("DateId", monotonically_increasing_id)
    val stateDimension = rawData.select(col("State")).distinct()
    val dimState=stateDimension.withColumn("StateId", monotonically_increasing_id)

    // Create the fact table
    val join1 = rawData.join(dimDate,Seq("Date"), "inner")
    val join2=join1.join(dimState,Seq("State"), "inner")
    val factData=join2.select("StateId","DateId", "TotalSamples",
    "Positive",
    "Negative")

    dimDate.write.mode("overwrite").option("header","true").csv(path_to_output_directory+"dimDate")
    dimState.write.mode("overwrite").option("header","true").csv(path_to_output_directory+"dimState")
    factData.write.mode("overwrite").option("header","true").csv(path_to_output_directory+"factData")


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
