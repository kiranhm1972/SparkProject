package main.java

import org.apache.spark.sql.SparkSession

object DemoDF {
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

    import sparkSession.implicits._
    println("\nDataFrame ---")
    val df1 = sparkSession.read.json("s3a://sankirtest/2011-01-17.json")
    df1.printSchema()
    df1.show(false)

    println("\nDataset ---")
    val ds1 = df1.select("_p.data.*").as[RetailCase]
    ds1.printSchema()
    ds1.show(false)




  }

}
