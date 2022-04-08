package main.java

import org.apache.spark.sql.SparkSession

object SparkExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkTest").getOrCreate()

    val sc = sparkSession.sparkContext

    val intrdd = sc.parallelize(List(10,50,20,40,30))
    intrdd.collect().foreach(println)

    val wordrdd = sc.parallelize(List("Red", "Blue", "Green", "Orange", "Yellow"))
    wordrdd.collect().foreach(println)
    println()

    val dfcsv = sparkSession.read.csv("data/retailinput.csv")
    dfcsv.printSchema()
    dfcsv.show()





  }

}
