package org.spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions.unix_timestamp

object example3SFO {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Example-3_SFO")
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

    //Saving DataFrame as a parquet file
    val parquetPath = "data/fireDFCallsparquetFile"
    fireDF
      .write
      .format("parquet")
      .mode("overwrite")
      .save(parquetPath)

    //Saving DataFrame as a table
    val parquetTable = "fireDFCallsparquetTable"
    fireDF
      .write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(parquetTable)

    // Transformations and actions
    val fewFireDF = fireDF
                      .select("IncidentNumber","AvailableDtTm", "CallType")
                      .where($"CallType" =!= "Medical Incident")
                      //.filter(col("CallType") =!= "Medical Incident")

    fewFireDF.show(5, false)

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct('CallType) as 'DistinctCallTypes)
      .show()

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)

    //Renaming Adding and Dropping columns
    val newFireDF = fireDF.withColumnRenamed("Delay",
                                                   "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    // Note: to_timestamp function is available to columns after Spark 2.2 and above,
    // Cloudxlab provides a Spark 2.1
/*
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")
*/
    val fireTsDF = newFireDF
                  .withColumn("IncidentDate", unix_timestamp(col("CallDate"), "MM/dd/yyyy").cast("timestamp"))
                  .drop("CallDate")
                  .withColumn("OnWatchDate", unix_timestamp(col("WatchDate"), "MM/dd/yyyy").cast("timestamp"))
                  .drop("WatchDate")
                  .withColumn("AvailableDtTS", unix_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a").cast("timestamp"))
                  .drop("AvailableDtTm")
    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    fireTsDF
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    //Common Dataframe Options
    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()
  }
}
