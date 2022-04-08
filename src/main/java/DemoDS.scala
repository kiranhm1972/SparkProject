package main.java

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object DemoDS {
  val recordSchema = StructType(Array(
    StructField("InvoiceNo", StringType),
    StructField("StockCode", StringType),
    StructField("Description", StringType, nullable = false),
    StructField("Quantity", IntegerType, nullable = false),
    StructField("InvoiceDate", TimestampType, nullable = false),
    StructField("UnitPrice", DoubleType, nullable = false),
    StructField("CustomerID", DoubleType, nullable = false),
    StructField("Country", StringType, nullable = false)))

  case class RetailCase(InvoiceNo: String,
                        StockCode: String,
                        Description: String,
                        Quantity: BigInt,
                        InvoiceDate: String,
                        UnitPrice: Double,
                        CustomerID: Double,
                        Country: String)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkTest")
      .config("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val df1 = sparkSession.read.json("s3a://sankirtest/retail-data.json")
    println("DataFrame : Reading json ")
    df1.printSchema()
    df1.show(false)

    import sparkSession.implicits._
    val dsjson = sparkSession.read.schema(recordSchema).json("s3a://sankirtest/retail-data.json").as[RetailCase]
    println("Dataset : Reading json ")
    dsjson.printSchema()
    dsjson.show(false)

  }
}
