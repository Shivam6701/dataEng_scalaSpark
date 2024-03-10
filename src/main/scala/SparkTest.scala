package org.example.shivam



  import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, functions}
  import org.apache.spark.sql.Encoders
  import org.apache.spark.sql.expressions.{Window, WindowSpec}
  import org.apache.spark.sql.functions.{col, regexp_replace, to_date, when, window}
  import org.apache.spark.sql.types.IntegerType
  //import org.apache.spark.sql.functions.{date_format, from_unixtime, month, months, to_date, unix_timestamp}
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
  import org.apache.spark.sql.functions.{year, month}
  import java.sql.Date


object SparkTest {
  val dw_dir = "file:///C:\\Users\\shiva\\Documents\\pyspark_training-main\\datasets\\dw_dataset"
  val sales_1_path = dw_dir + "\\sales_1.csv"
  val sales_2_path = dw_dir + "\\sales_2.csv"
  val product_path = dw_dir + "\\product_meta.csv"



  def simple_df(spark: SparkSession): Unit = {


    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    sales1Df.printSchema()
    val df2 = sales1Df.select("item_qty", "unit_price", "total_amount")
    val df3 = df2.withColumn("actual_total", df2.col("item_qty") * df2.col("unit_price"))
    val df4 = df3.withColumn("discount", df3.col("actual_total") - df3.col("total_amount"))

    //df4.show
    val sumTotal = df4.agg(Map("total_amount" -> "sum", "discount" -> "sum")).withColumnsRenamed(
      Map("sum(total_amount)" -> "total_amount_sum", "sum(discount)" -> "discount_sum")
    )
    val pctTotal = sumTotal.withColumn("pct_total", sumTotal.col("discount_sum") / sumTotal.col("total_amount_sum"))
    val values: Row = pctTotal.first()
    val amount: Long = values.getLong(0)
    val discount: Long = values.getLong(1)
    pctTotal.show()
    println(amount + " " + discount)

    //sales1Df.show()



    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    //sales2Df.printSchema()
    val unionDf = sales1Df.union(sales2Df)
    //println(unionDf.count())

    val product_path = dw_dir + "\\product_meta.csv"

    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinDf = prodDf.join(unionDf, "item_id")
    joinDf.show()
    joinDf.explain(extended = true)
  }

  def complex_join(spark: SparkSession): Unit = {


    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    //sales1Df.printSchema()
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)

    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)

    val unionDf = sales1Df.union(sales2Df)

    val df2 = sales1Df.select("item_qty", "unit_price", "total_amount")
    val df3 = unionDf.withColumn("actual_total", df2.col("item_qty") * df2.col("unit_price"))
    val transformedSalesDf = df3.withColumn("discount", df3.col("actual_total") - df3.col("total_amount")).filter("unit_price>1")


    val joinedDf=prodDf.join(transformedSalesDf,"item_id")
    val groupDf=joinedDf.groupBy("product_type").sum("total_amount")
    groupDf.explain(extended = true)

  }

  case class Sales(item_id: Int, item_qty: Int, unit_price: Int, total_amount: Int, date_of_sale: Date)
  def dataset_version(spark: SparkSession): Unit = {
    import spark.implicits._
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val collectedSales2: Array[Row] = sales2Df.collect()
    val row1: Row = collectedSales2(0)
    val item_id = row1.getInt(0)
    val item_qty = row1.getInt(1)
    val expectedTotalQ = row1.getInt(2) * row1.getInt(3)

    val salesDs: Dataset[Sales] = sales1Df.as[Sales]
    val finalDs = salesDs.filter(x => x.unit_price > 10)
    val collectedDs: Array[Sales] = salesDs.collect()
    val sales_row1 = collectedDs(0)
    val expectedTotal = sales_row1.item_qty * sales_row1.unit_price


    finalDs.show()
    //salesDs.show()
  }
  def sql_version(spark: SparkSession): Unit = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    sales1Df.createOrReplaceTempView("sales_tbl")
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    prodDf.createOrReplaceTempView("prod_tbl")

    spark.sql("select t.* from  prod_tbl t left outer join sales_tbl s on t.item_id=s.item_id"+" where s.item_id is not null" ).show()
    spark.sql("select * from prod_tbl t left semi join sales_tbl s on t.item_id=s.item_id").show()

    spark.sql("select t.* from  prod_tbl t left outer join sales_tbl s on t.item_id=s.item_id" + " where s.item_id is null").show()
    spark.sql("select * from prod_tbl t left anti join sales_tbl s on t.item_id=s.item_id").show()
  }

  def window_agg(spark:SparkSession):Unit={
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val rankSpec: WindowSpec = Window.partitionBy("date_of_sale").orderBy("total_amount")
    val rankWindowDf:Dataset[Row]=sales1Df.withColumn("date_wise_rank",functions.rank.over(rankSpec))
    rankWindowDf.show()
    val rowSpec=Window.partitionBy().orderBy("item_id")
    val rowWindowDf=sales1Df.withColumn("row_number",functions.row_number.over(rowSpec))
    rowWindowDf.show()

  }

  def simple_cube(spark: SparkSession) = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.show()
    val cubedDf = joinedDf.cube("product_name", "date_of_sale").sum()
    cubedDf.orderBy("product_name").show()

  }

  def joined_write(spark: SparkSession) = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.write.mode("overwrite")
      .option("header", "true")
      .parquet("C:\\Users\\shiva\\Documents\\DataEng\\output\\joined_csv")

    val newJoinedDf = spark.read.parquet("file:///C:\\Users\\shiva\\Documents\\DataEng\\output\\joined_csv")
    val groupedDf = newJoinedDf.groupBy("item_id").sum("total_amount")
    groupedDf.show()
    val groupd2Df = joinedDf.groupBy("item_id").sum("total_amount")
    groupd2Df.show()
    Thread.sleep(1000000)

  }
  def assessment1(spark: SparkSession):Unit={
    val sales_data_path= "file:///C:\\Users\\shiva\\Documents\\DataEng\\asses1\\dataset-1\\dataset\\Global Superstore Sales - Global Superstore Sales.csv"
    val return_data_path="file:///C:\\Users\\shiva\\Documents\\DataEng\\asses1\\dataset-1\\dataset\\Global Superstore Sales - Global Superstore Returns.csv"
    val salesDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_data_path)
    val returnDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(return_data_path)

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df1=salesDf.withColumn("date", to_date(col("Order Date"), "MM/dd/yyyy"))
    val df2=df1.withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))

    val df3=df2.withColumn("Profit Price",regexp_replace(col("Profit"),"\\$",""))
            .withColumn("Profit Price",col("Profit Price").cast("Float"))

    val df4=df3.filter(df3("Returns")==="No")
    val windowSpec= Window.partitionBy("year","month","Category","Sub-Category")

    val finalDf=df4.groupBy("year","month","Category","Sub-Category")
      .sum("Quantity","Profit Price")
      .orderBy("year","month","Category","Sub-Category")
      .withColumnRenamed("sum(Quantity)","Total Quantity Sold")
      .withColumnRenamed("sum(Profit Price)","Total Profit")




    finalDf.write.mode("overwrite")
      .partitionBy("year", "month")
      .format("csv")
      .save("C:\\Users\\shiva\\Documents\\DataEng\\asses1\\output1")

    finalDf.show()


  }


  def main(args: Array[String]) = {
    println("Hello!")


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

    //simple_df(spark)
    //complex_join(spark)
    //dataset_version(spark)
    //sql_version(spark)
    //window_agg(spark)
    //simple_cube(spark)
    joined_write(spark)
    //assessment1(spark)
  }
}