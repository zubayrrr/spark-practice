package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Creating columns, columns are special objects that will allow you to obtain new DFs out of some source DFs by processing the values inside
  val firstColumn = carsDF.col("Name")

  // selecting, (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    // there is a small difference between doing this and doing carsDF.col() but they both do the same thing
    'Year, // Scala Symbol, auto-converted to column, single quotes
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )
  // select with plain column names
  // you can either pass Column objects and expressions OR column names as strings
  carsDF.select("Name", "Year")

  // EXPRESSIONS are powerful construct that allows you to process DFs in any fashion you like

  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    // we can also use
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2")
    // has different implementation of the division operator
  )
  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF Processing

  // Adding a new column to an existing DF as a result obtaining a new DF
  val carsWithKgs3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs / 2.2"))

  // Renaming an existing column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

  // space and hyphens cannot be used, use backticks ` ` for escaping characters
//  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

 // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
 // != would come in conflict with Scala operator, use =!= instead

 // where
 val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA' ")

  // equals filter using col expression
  val europeanCarsDF3 = carsDF.where(col("Origin") === "USA")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)

  // combining filters into one using .and
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  // and without parenthesis
  val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  // expression strings
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // Unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same Schema

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

}
