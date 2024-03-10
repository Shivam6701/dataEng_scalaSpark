package org.example.shivam

import org.apache.spark.sql.functions.{expr, window}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}



object StreamingDemo {

  def stream_demo(spark: SparkSession): Unit = {
    val streamingFiles = spark.readStream.text("file:///C:\\Users\\shiva\\Documents\\DataEng\\temp\\text_files")
    val words = streamingFiles.select(functions.explode(functions.split(streamingFiles.col("value"), " ")).alias("word"))
    val wordCount = words.groupBy("word").count().alias("word_total")
    val query = wordCount.writeStream.outputMode("complete").format("console").start
    //val query = words.writeStream.outputMode("append").format("console").start
    query.awaitTermination()

  }

  def join_static(spark: SparkSession): Unit = {
    val wordType = spark.read.option("header", "true").csv("file:///C:\\Users\\shiva\\Documents\\DataEng\\temp\\ne.csv")

    val streamingFiles = spark.readStream.text("file:///C:\\Users\\shiva\\Documents\\DataEng\\temp\\text_files")
    val words = streamingFiles.select(functions.explode(functions.split(streamingFiles.col("value"), " ")).alias("word"))
    val joinedWords=words.join(wordType,words.col("word")===wordType.col("word"),"left_outer")

    val wordCount = joinedWords.groupBy("type").count().alias("type_total")
    val query = wordCount.writeStream.outputMode("complete").format("console").start
    //val query = words.writeStream.outputMode("append").format("console").start
    query.awaitTermination()

  }

  def read_news(spark: SparkSession): DataFrame = {
    val newsSchema: StructType = new StructType()
    .add(StructField("ticker", DataTypes.StringType,false))
    .add(StructField("news", DataTypes.StringType,false))
    .add(StructField("date_of_news", DataTypes.TimestampType,true))

    val news = spark.readStream.schema(newsSchema)
      .option("header", "true").json("file:///C:\\Users\\shiva\\Documents\\DataEng\\temp\\news")
    news
    //val query = news.writeStream.outputMode("append").format("console").start

    //query.awaitTermination()

  }

  def read_price(spark: SparkSession): DataFrame = {
    val priceSchema: StructType = new StructType()
    .add(StructField("ticker", DataTypes.StringType, false))
    .add(StructField("price", DataTypes.StringType, false))
    .add(StructField("price_time", DataTypes.TimestampType, true))

    val price = spark.readStream.schema(priceSchema)
      .option("header", "true").csv("file:///C:\\Users\\shiva\\Documents\\DataEng\\temp\\price")

    //val query = price.writeStream.outputMode("append").format("console").start

    //query.awaitTermination()
    price

  }

  def join_stream(spark:SparkSession):Unit={
    val priceDf = read_price(spark)
    val newsDf = read_news(spark)

    val joinedDf = priceDf.join(newsDf, priceDf.col("ticker") === newsDf.col("ticker"))
    val query = joinedDf.writeStream.outputMode("append").format("console").start
    query.awaitTermination()


  }
  def join_straming_with_watermark(spark:SparkSession):Unit={

    val priceDf = read_price(spark)
    val priceDfW = priceDf.withColumnRenamed("ticker", "price_ticker")
      .withWatermark("price_time", "1 minutes")
    priceDf.printSchema()
    val newsDf = read_news(spark)
    val newsDfW = newsDf.withWatermark("date_of_news", "1 minutes")
    val joinedDf = priceDfW.join(newsDfW, expr(
      "price_ticker = ticker AND " +
        "price_time >= date_of_news AND " +
        "price_time <= date_of_news + interval 1 hour ")
    )


    //    (priceDfW.col("ticker") === newsDfW.col("ticker")).and(
    //      priceDfW.col("price_time").leq(newsDfW.col("date_of_news")
    //        .plus("60")))

    val query = joinedDf.writeStream.outputMode("append").format("console").start
    query.awaitTermination()
  }

def window_groupby(spark:SparkSession):Unit={
  val priceDf=read_price(spark)
  val groupedDf=priceDf.groupBy(window(priceDf.col("ticker"),
    "10 minutes","5 minutes")).avg("price")
  val query = groupedDf.writeStream.outputMode("append").format("console").start
  query.awaitTermination()
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

    //    val streamingFiles = spark.read.text("file:///C:\\tmp\\text_files")
    //    streamingFiles.show()
    //join_static(spark)
    //read_news(spark)
    //read_price(spark)
    //join_stream(spark)
    //join_straming_with_watermark(spark)
    //window_groupby(spark)


  }
}
