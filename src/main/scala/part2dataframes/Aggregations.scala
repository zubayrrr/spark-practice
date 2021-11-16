package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, min, stddev, sum, mean}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // counting
  // selecting all different genres
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null

  // using expr
  moviesDF.selectExpr("count(Major_Genre)")

  // count all the rows
  moviesDF.select(count("*")).show() // count all including nulls
  genresCountDF.show()

  // counting distinct values
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count - will not scan entire DB row by row but rather gets you an approximate row count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()


/*
Grouping
 */

  // we want to not only count but compute how many movies we have for each of those genres

  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")) // includes null
  // when you call by groupBy you obtain  a relational grouped dataset
  // you also need to call an aggregation on this groupBy
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  // compute avg IMDB_Rating by Genre

  val acgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  // alternative
  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()


/*
Exercises
1. Sum up ALL the profits of ALL the movies in the DF
2. Count how many distinct directories we have
3. Show the mean and standard deviation of US Gross revenue for the movies
4. Compute the average IMDB rating and the average US Gross revenue PER DIRECTOR
*/

  // 1

  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2

  moviesDF.select(countDistinct(col("Director"))).show()

  // 3

  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4

  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy(col("Avg_Rating").desc_nulls_last) .show()

  // the exercises performed above are Wide Transformations


}
