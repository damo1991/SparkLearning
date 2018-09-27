package Dataframe

import org.apache.spark.sql.SparkSession

/**
  * Created by damodharraop on 9/26/2018.
  */
object Basic {

  case class Cust(id:Integer,name:String,sales:Double,discount:Double,state:String)

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("basic").master("local[*]").getOrCreate()
    import spark.implicits._

    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** toString() just gives you the schema")

    //println(customerDF.toString())
customerDF.printSchema()
    println(s"showing the data in the table")
    customerDF.show()
    println(s"geting all id's in the table ${customerDF.select($"id").show()}")
    println(s"selecting  multiple columns in the table ${customerDF.select($"id",$"name").show()}")
    customerDF.filter($"state".equalTo("CA")).show()






  }
}
