package org.example.shivam


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, month, regexp_replace, round, to_date, year}


object Test1{
  val sales_data_path = "file:///C:\\Users\\shiva\\Documents\\DataEng\\asses1\\dataset-1\\dataset\\Global Superstore Sales - Global Superstore Sales.csv"
  val return_data_path = "file:///C:\\Users\\shiva\\Documents\\DataEng\\asses1\\dataset-1\\dataset\\Global Superstore Sales - Global Superstore Returns.csv"
  val output_path="C:\\Users\\shiva\\Documents\\DataEng\\asses1\\output1"
  def assessment1(spark: SparkSession): Unit = {
    val salesDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_data_path)
    val returnDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(return_data_path)
    //fix date format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df1 = salesDf.withColumn("date", to_date(col("Order Date"), "MM/dd/yyyy"))
    val df2 = df1.withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))
    //fix Profit symbol
    val df3 = df2.withColumn("Profit Price", regexp_replace(col("Profit"), "\\$", ""))
      .withColumn("Profit Price", col("Profit Price").cast("double"))
    //reject returned data
    val df4 = df3.filter(df3("Returns") === "No")

    val finalDf = df4.groupBy("year", "month", "Category", "Sub-Category")
      .sum("Quantity", "Profit Price")
      .orderBy("year", "month", "Category", "Sub-Category")
      .withColumnRenamed("sum(Quantity)", "Total Quantity Sold")
      .withColumnRenamed("sum(Profit Price)", "Total Profit")
      .withColumn("Total Profit",round(col("Total Profit"),2))


    finalDf.write.mode("overwrite")
      .partitionBy("year", "month")
      .format("csv")
      .save(output_path)

    finalDf.show()


  }


  def main(args: Array[String]) = {

    val winutilPath = "C:\\software\\winutils\\hadoop-3.3.0" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    assessment1(spark)
  }
}