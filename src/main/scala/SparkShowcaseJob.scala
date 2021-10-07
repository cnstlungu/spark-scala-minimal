import org.apache.spark.sql.DataFrame

object Main extends App with SparkProdContext {

  import ss.implicits._

  def differenceDatasets(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.exceptAll(df2)
  }

  def unionDatasets(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.union(df2).distinct()
  }

  def intersectionDatasets(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.intersect(df2)
  }

  println("Running dataframe operations")

  val df1 = Seq(
    ("John", "Johnson", 17),
    ("Henry", "Petrovich", 18),
    ("Harry", "Harrison", 19)
  ).toDF("first_name", "last_name", "age")

  val df2 = Seq(
    ("Henry", "Petrovich", 18),
    ("Harry", "Harrison", 19),
    ("Sequoia", "Johnson", 25)
  ).toDF("first_name", "last_name", "age")

  val result = intersectionDatasets(df1, df2)

  result.collect.foreach(println(_))

  sc.stop
  println("Processing complete")

}
