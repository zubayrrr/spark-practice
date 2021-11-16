package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // how to add a certain value to a column(plain value)
  moviesDF.select(col("Title"), lit(74).as("plain_value"))//.show() // literally 74 to every single row
  // lit with work with any type of values

  // Booleans
  val dramaFilter = col ("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)//.where(col("Major_Genre") === "Drama") // or equalTo "Drama"
  // + multiple ways of filtering
  //moviesDF.select(col("Title"),preferredFilter.as("good_movie")).show() // these boolean values can be evaluated as actual values inside the DFs

  // filter on a boolean column
  val moviesWithGoodnessFlagDF =  moviesDF.select(col("Title"),preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagDF.where("good_movie") // essentially doing where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagDF.where(not(col("good_movie")))

  // numbers
  // numerical types
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  // Spark will throw exception if the columns are not numerical

  // math operators
  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION

  // Strings

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
  // capitalization
  carsDF.select(initcap(col("Name"))).show() // initcap will capitalize the first letter of every word contained in the respective column
  // lower, upper

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  //).where(col("regex_extract") =!= "").show()
  // if in practice you simply wanted to extract all the volkswagen in your carsDF
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()

  /*
  * Exercise
  * Filter the cars DF by a list of car names obtained by an API call
  * */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volkswagen|mercedes-benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract").show()

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()


}
