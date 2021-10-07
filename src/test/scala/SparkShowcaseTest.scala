import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SparkShowcaseTest extends AnyFunSuite with SparkTestContext {

  import ss.implicits._

  test("Scala Showcase - DataFrame Difference") {
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


    val actual = Main.differenceDatasets(df1, df2).collect()

    val expected = Seq(
      ("John", "Johnson", 17)
    ).toDF("first_name", "last_name", "age").collect()

    actual should contain theSameElementsAs expected
  }

  test("Scala Showcase - DataFrame Union") {
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


    val actual = Main.unionDatasets(df1, df2).collect()

    val expected = Seq(
      ("John", "Johnson", 17),
      ("Henry", "Petrovich", 18),
      ("Harry", "Harrison", 19),
      ("Sequoia", "Johnson", 25)
    ).toDF("first_name", "last_name", "age").collect()

    actual should contain theSameElementsAs expected
  }

  test("Scala Showcase - DataFrame Intersection") {
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

    val actual = Main.intersectionDatasets(df1, df2).collect()

    val expected = Seq(
      ("Henry", "Petrovich", 18),
      ("Harry", "Harrison", 19)
    ).toDF("first_name", "last_name", "age").collect()

    actual should contain theSameElementsAs expected
  }

}