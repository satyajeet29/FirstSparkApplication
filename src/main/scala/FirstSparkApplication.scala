package org.spark.practice
import org.apache.spark.{SparkConf,SparkContext}

object FirstSparkApplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)
    val rdd1  =sc.makeRDD(Array(1,2,3,4,5,6))
    rdd1.collect().foreach(println)

  }
}
