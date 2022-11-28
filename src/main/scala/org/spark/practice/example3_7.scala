package org.spark.practice
// Not executed
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object example3_7 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Example-3_7")
      .getOrCreate()
    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    // Get the path to the JSON file
    val jsonFile = args(0)
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}
