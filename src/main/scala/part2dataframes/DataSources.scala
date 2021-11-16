package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
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
    StructField("Year", DateType),
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

  // JSON flags
  spark.read
    .format("json")
    // flags specific to JSON
    .schema(carsSchema) // Date needs to be DateType on Schema
    .option("dateFormat", "YYYY-MM-dd") // works on only enforced schema, if format not mentioned Spark will use the ISO format
    // couple with Schema; if Spark fails parsing, it will put null
    // timestamp formats, time upto seconds precision
    .option("allowSingeQuotes", "true")
    .option("compression", "uncompressed") // default is uncompressed, other options are bzip2, gzip, lz4, snappy, deflate
    //.load("src/main/resources/data/cars.json")
    .json("src/main/resources/data/cars.json")

  // CSV flags (has the most possible flags)
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",") // separator
    .option("nullValue", "") // there is no notion of null values in CSV, this option will instruct Spark to parse "" as null in resulting DF
    // there are many other options
   // .load("src/main/resources/data/stocks.csv")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet - Open source compressed binary storage format optimized for fast reading of columns, works very well in Spark(default for storing DFs)
  // it is very predictable(less options passed)
  carsDF.write
    //.format("parquet") isn't necessary as its the default
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet") // or
  //.save("path") since parquet is default

  // Text file
  // Every single line in the txt file will be considered a value in a single column data frame

  spark.read.text("src/main/resources/data/SampleText.txt").show()

  // Reading from a remote database
  // is a common pattern, data is often migrated from DBses to Spark for analysis etc

  // the postgres DB needs to be spun up sudo docker-compose up

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()
  employeesDF.show()

  /*
  Exercise: read the movies DF, the write it as
  - tab-separated values file
  - snappy Parquet
  - table in the Postgres DB public.movies
  */

//  val moviesDF = spark.read
//    .format("json")
//    .load("src/main/resources/data/movies.json")
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

//TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DB
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
