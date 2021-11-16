package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // loading DFs
  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  // Doing Join on DFs

  // to reuse expressions are extracted as values
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner") // inner, don't need to pass as its default
  guitaristsBandsDF//.show()

  // outer joins

  // left outer = everything in the inner join + all the rows in the LEFT table (guitarsDF), with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")//.show()

  // right outer = everything in the inner join + all the rows in the RIGHT table (bandsDF), with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")//.show()

  // full outer join = everything in the inner join + all the rows in the both table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")//.show()

  // semi-joins
  // only show rows in the LEFT DF for which there is a row satisfying the join condition
  // its like doing an inner join but cut out data from the RIGHT DF and keep data just for the LEFT DF
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-joins
  // only keep rows from the LEFT DF for which there are NO row in RIGHT DF satisfying the join condition
  // they basically give back the missing rows from the LEFT DF
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // points to bear in mind
  // in the inner join above, both the bands DF has a column ID and the guitarist DF has column ID
  // guitaristsBandsDF.select("id","band").show // column IDs are ambiguous and Spark would crash

  // to solve

  // option 1 - rename the column on which we're joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))
  // spark maintains a unique identifier on all the columns it operates on

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  // joins using arrays
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /*
  * Exercises
  * - show all employees and their max salaries
  * - show all employees who were never managers
  * - find the job titles of the best paid 10 employees
  * */


  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()
  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1

  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  //employeesSalariesDF.show()

  // 2  using an anti-join

  val empNeverManagerDF = employeesDF.join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "left_anti")
  //empNeverManagerDF.show()

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()
}


