package Dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by damodharraop on 9/28/2018.
  */
object DatasetConversion {

  case class Cust(id:Integer,name:String,sales:Double,discount:Double,state:String)
  case class statesales(state:String,sales:Double)

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("DatasetConversion").getOrCreate()
    import spark.implicits._
    val custs=Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val customerDF: DataFrame =spark.createDataFrame(custs)

    println("*** DataFrame schema")

    customerDF.printSchema()

    println("*** DataFrame contents")

    customerDF.show()

    println("*** Select and filter the DataFrame")

    val smallerDF =
      customerDF.select("sales", "state").filter($"state".equalTo("CA"))

    smallerDF.show()


    val customerDS:  Dataset[statesales]=smallerDF.as[statesales]
    println("*** Dataset schema")

    customerDS.printSchema()

    println("*** Dataset contents")

    customerDS.show()

    val verySmallDS: Dataset[Double]=customerDS.select($"sales".as[Double])

    println("*** Dataset after projecting one column")

    verySmallDS.show()
    // If you select multiple columns on a Dataset you end up with a Dataset
    // of tuple type, but the columns keep their names.
    val tupleDS : Dataset[(String, Double)] =
    customerDS.select($"state".as[String], $"sales".as[Double])

    println("*** Dataset after projecting two columns -- tuple version")

    tupleDS.show()

    // You can also cast back to a Dataset of a case class. Notice this time
    // the columns have the opposite order than the last Dataset[StateSales]
    val betterDS: Dataset[statesales] = tupleDS.as[statesales]

    println("*** Dataset after projecting two columns -- case class version")

    betterDS.show()

    // Converting back to a DataFrame without making other changes is really easy
    val backToDataFrame : DataFrame = tupleDS.toDF()

    println("*** This time as a DataFrame")

    backToDataFrame.show()

    // While converting back to a DataFrame you can rename the columns
    val renamedDataFrame : DataFrame = tupleDS.toDF("MyState", "MySales")

    println("*** Again as a DataFrame but with renamed columns")

    renamedDataFrame.show()
  }

}
