
package org.example.shivam
import org.apache.spark.sql.functions.{avg, col, expr, mean, window}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}



object Test2 {

  def read_Iot_temp(spark: SparkSession): DataFrame = {
    val Iot_temp_Schema: StructType = new StructType()
      .add(StructField("device_id", DataTypes.StringType, false))
      .add(StructField("temperature", DataTypes.StringType, false))
      .add(StructField("timestamp", DataTypes.TimestampType, true))

    val temp = spark.readStream.schema(Iot_temp_Schema)
      .option("header", "true").csv("file:///C:\\Users\\shiva\\Documents\\DataEng\\asses2\\ioasses2\\inputs")
    temp


  }


  def TempMonitor(spark: SparkSession): Unit = {
    val temp_df=read_Iot_temp(spark)

    val max_temp = spark.read.option("header", "true").csv("file:///C:\\Users\\shiva\\Documents\\DataEng\\asses2\\ioasses2\\max_temp.csv")
    val new_max_temp=max_temp.withColumn("max_temp",col("max_temp").cast(DataTypes.LongType))
    val new_temp_df=temp_df.withColumn("temperature",col("temperature").cast(DataTypes.LongType))
   val joinedDf = new_temp_df.join(new_max_temp, Seq("device_id"), "leftouter")



    val jd3=joinedDf.filter(
      joinedDf.col("temperature")>joinedDf.col("max_temp") )

    val query = jd3.writeStream.outputMode("append").format("console").start


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

    TempMonitor(spark)


  }


}
