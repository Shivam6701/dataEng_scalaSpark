package org.example.shivam

import org.apache.spark.sql.functions.{explode, explode_outer}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DfCode {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }



    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.read.text()

    import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    //    val df2 = spark.read.schema(arraySchema).csv()
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)
    df.select($"name",explode($"knownLanguages")).show(false)

    df.select($"name",explode_outer($"knownLanguages"))
      .show(false)
  }

}
