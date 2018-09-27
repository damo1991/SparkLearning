package Dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by damodharraop on 9/26/2018.
  */
object ComplexSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Complex schema").master("local[*]").getOrCreate()
    val row1 = Seq(
      Row(1, Row("a", "b"), 8.00, Row(1, 2)),
      Row(2, Row("c", "d"), 8.00, Row(3, 4))
    )

    val rowRdd = spark.sparkContext.parallelize(row1, 4)

    val schema = StructType(
      Seq(StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ),true
        ),
        StructField("d", DoubleType, true),
        StructField("s2",
          StructType(
            Seq(
              StructField("u", IntegerType, true),
              StructField("v", IntegerType, true)

            )
          )
,true
        )

      )
    )

    val df1=spark.createDataFrame(rowRdd,schema)

    df1.printSchema()

    println("Select the column with nested Row at the top level")
    df1.select("s1").show()



  }

}
