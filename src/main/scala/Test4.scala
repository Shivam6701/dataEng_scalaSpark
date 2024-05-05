package org.example.shivam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, date_format, regexp_replace, to_date, to_timestamp, year}
import org.apache.spark.streaming.StateSpec.function


object Test4{
  val apple_data = "C:\\Users\\shiva\\Documents\\DataEng\\asses4\\inputs\\aapl_2014_2023.csv"
  val bitcoin_data="C:\\Users\\shiva\\Documents\\DataEng\\asses4\\inputs\\Bitcoin Historical Data2.csv"
  val eth_data="C:\\Users\\shiva\\Documents\\DataEng\\asses4\\inputs\\Ethereum Historical Data2.csv"
  val out_tbl="C:\\Users\\shiva\\Documents\\DataEng\\asses4\\outputs"
  def process()(implicit spark:SparkSession):Unit={
    val appDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(apple_data)
    val bitDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(bitcoin_data)
    val ethDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(eth_data)
    //appDf.printSchema()
    val app1Df=appDf.withColumn("year", year(to_timestamp(col("date"), "YYYY-MM-DD")))
      .withColumn("close",regexp_replace(col("close"), ",", ""))
    val windowSpec = Window.partitionBy("year").orderBy("date").rowsBetween(-19, 0)
    val result = app1Df
      .withColumn("apple_20day_avg", avg("close").over(windowSpec))
    val result1=result.select("year","date","high" ,"low","close","apple_20day_avg")
    .withColumnRenamed("high","apple_high")
   .withColumnRenamed("low","apple_low")
   .withColumnRenamed("close","apple_close")

    //spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val bit3df= bitDf.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))
      .withColumn("Price",regexp_replace(col("Price"), ",", ""))
    bit3df.show()
    val bit1Df=bit3df.withColumn("Year", year(to_timestamp(col("Date"), "yyyy-MM-DD")))
    val windowSpecBit = Window.partitionBy("Year").orderBy("Date").rowsBetween(-19, 0)

    val bitresult = bit1Df
      .withColumn("bitcoin_20day_avg", avg("Price").over(windowSpecBit))
    val bitresult1 = bitresult.select( "Date", "High", "Low", "Price", "bitcoin_20day_avg")
   .withColumnRenamed("High", "bitcoin_high")
    .withColumnRenamed("Low", "bitcoin_low")
    .withColumnRenamed("Price", "bitcoin_price")
      .withColumnRenamed("Date","bitdate")
      //.filter(col("Year")>= 2018 && col("Year")<=2023 )
    bitresult1.show()


    val final_result1=bitresult1.join(result1, result1.col("date") === bitresult1.col("bitdate"),"inner")
    val eth3df = ethDf.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))
      .withColumn("Price",regexp_replace(col("Price"), ",", ""))
    //bit3df.show()
    val eth1Df = eth3df.withColumn("Year", year(to_timestamp(col("Date"), "yyyy-MM-DD")))
    val windowSpeceth = Window.partitionBy("Year").orderBy("Date").rowsBetween(-19, 0)

    val ethresult = eth1Df
      .withColumn("ethereum_20day_avg", avg(col("Price")).over(windowSpeceth))
    val ethesult1 = ethresult.select( "Date", "High", "Low", "Price", "ethereum_20day_avg")
      .withColumnRenamed("High", "ethereum_high")
      .withColumnRenamed("Low", "ethereum_low")
      .withColumnRenamed("Price", "ethereum_price")
      .withColumnRenamed("Date","ethdate")

    val final_result2=final_result1.join(ethesult1, ethesult1.col("ethdate") === final_result1.col("date"),"inner")
    val finaldf=final_result2.select("Year","date","apple_high","apple_20day_avg","apple_low","bitcoin_high","bitcoin_20day_avg","bitcoin_low","ethereum_high","ethereum_20day_avg","ethereum_low" )
      .filter(col("Year")>= 2018 && col("Year")<=2023 )

    //finaldf.printSchema()
    finaldf.show()
    finaldf.write.mode("overwrite")
      .option("header","true")
      .format("csv")
      .save(out_tbl)

  }


  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\software\\winutils\\hadoop-3.3.0" //"C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }


   implicit val spark = SparkSession.builder
      .appName("Simple Stream Application")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")
 process()


  }


}