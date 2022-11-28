package org.spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions.unix_timestamp

object example3_EndToEnd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("CH-3_EndToEnd")
      .getOrCreate()

    import spark.implicits._

    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    // Get the path to the JSON file
    val sfFireFile = args(0)
    val fire_schema = StructType(
      Array(
        StructField("CallNumber", IntegerType, true),
        StructField("UnitID", StringType, true),
        StructField("IncidentNumber", IntegerType, true),
        StructField("CallType", StringType, true),
        StructField("CallDate", StringType, true),
        StructField("WatchDate", StringType, true),
        StructField("CallFinalDisposition", StringType, true),
        StructField("AvailableDtTm", StringType, true),
        StructField("Address", StringType, true),
        StructField("City", StringType, true),
        StructField("Zipcode", IntegerType, true),
        StructField("Battalion", StringType, true),
        StructField("StationArea", StringType, true),
        StructField("Box", StringType, true),
        StructField("OriginalPriority", StringType, true),
        StructField("Priority", StringType, true),
        StructField("FinalPriority", StringType, true),
        StructField("ALSUnit", StringType, true),
        StructField("CallTyeGroup", StringType, true),
        StructField("NumAlarms", IntegerType, true),
        StructField("UnitType", StringType, true),
        StructField("UnitSequenceCallDispatch", StringType, true),
        StructField("FirePreventionDistrict", StringType, true),
        StructField("SupervisorDistrict", StringType, true),
        StructField("Neighborhood", StringType, true),
        StructField("Location", StringType, true),
        StructField("RowID", StringType, true),
        StructField("Delay", FloatType, true)
      )
    )
    var fireDF = spark.read
      .schema(fire_schema)
      .option("header", "true")
      .csv(sfFireFile)
      .withColumn("CallYear", year(unix_timestamp(col("CallDate"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("CallMonth", month(unix_timestamp(col("CallDate"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("CallWeek", weekofyear(unix_timestamp(col("CallDate"), "MM/dd/yyyy").cast("timestamp")))

    var FireDF2018 = fireDF.where($"CallYear" === 2018)

    // Different types of fire calls
    var callType2018 = FireDF2018.select($"CallType").distinct.show()

    //AggregateCounts by month for 2018
    FireDF2018.select($"CallNumber", $"CallMonth")
      .groupBy("CallMonth")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "MonthlyCount")
      .show(12, false)

    //Fire calls by Neighborhood
    FireDF2018.select($"CallNumber", $"Neighborhood")
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "NeighborCount")
      .show()

    //Calls by Week Year
    FireDF2018.select($"CallNumber", $"CallWeek")
      .groupBy("CallWeek")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "WeeklyCount")
      .show(12, false)

    //
    FireDF2018.select($"Delay", $"Neighborhood")
      .groupBy("Neighborhood")
      .max("Delay")
      .withColumnRenamed("max(Delay)", "maxDelay")
      .orderBy($"maxDelay".desc)
      .show()

    var corrDF = FireDF2018.select($"Neighborhood", $"Zipcode", $"CallNumber")
      .groupBy("Neighborhood", "Zipcode")
      .count()
      .withColumnRenamed("count", "Number")
      .orderBy($"Number".desc)

    corrDF.show()


  }
}
