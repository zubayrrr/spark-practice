package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("Spark.master", "local")
    .getOrCreate()

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

/*
Reading a DF:
- format
- schema (optional if inferSchema = true)
- zero or more options
*/

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce schema
    .option("mode", "failFast") // other ops dropMalformed, permissive(default)
    .load("src/main/resources/data/cars.json") // or
  // .otpion("path", "src/main/resources/data/cars.json")

  // another way of reading a DF without passing .option chain calls is to use an option map

  var carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failfast",
      "path" -> "src/main/resources/data/cars.json", // you could also pass in path from other sources like an S3 bucket
      "inferSchema" -> "true"
    )) // this also allows you to compute options dynamically at runtime
    .load()

 /*
  Writing DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path
  - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json") // or
   // .save("src/main/resources/data/cars_dupe.json")
    .save()

  /*
  cars_dupe.json will come out as a folder in which there will be partitioned file(s) name part-UUID.json
  */


}
