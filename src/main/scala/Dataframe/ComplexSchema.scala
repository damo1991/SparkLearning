package Dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by damodharraop on 9/26/2018.
  */
object ComplexSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Complex schema").master("local[*]").getOrCreate()
    import spark.implicits._
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
    println("Select deep into the column with nested Row")
    df1.select("s1.x").show()
    println("The column function getField() seems to be the 'right' way")
    df1.select($"s1".getField("x")).show()


    val row2=Seq(Row(1,Row("a","b"),8.00,Array(1,2)),
      Row(2,Row("c","d"),9.00,Array(3,4,5))

    )

    val rows2Rdd = spark.sparkContext.parallelize(row2, 4)

    val schema2=StructType(
      Seq(
        StructField("id",IntegerType,true),
        StructField("s1",StructType(
          Seq(StructField("x",StringType,true),
            StructField("y",StringType,true))
          )
          ,true)
      )
    ).add(StructField("d", DoubleType, true))
      .add("a", ArrayType(IntegerType))

    val df2=spark.createDataFrame(rows2Rdd,schema2)
    println("scheme with array")
    df2.printSchema()

    println("DataFrame with array")
    df2.show()

    df2.select($"id",size($"a").as("count")).show()

    println("Explode the array elements out into additional rows")
    df2.select($"id", explode($"a").as("element")).show()

    println("Apply a membership test to each array in a column")
    df2.select($"id", array_contains($"a", 2).as("has2")).show()

    println("Use column function getItem() to index into array when selecting")
    df2.select($"id", $"a".getItem(2)).show()

    val row3=Seq(
      Row(1,8.00,Map("u"->1,"v"->2)),
      Row(2,9.00,Map("x"->3,"y"->4,"z"->5))
    )

    val row3Rdd=spark.sparkContext.parallelize(row3)

    val schema3=StructType(
      Seq(
        StructField("id",IntegerType,true),
        StructField("d",DoubleType,true),
        StructField("m",MapType(StringType,IntegerType))
      )
    )
    val df3=spark.createDataFrame(row3Rdd,schema3)
    println("Schema with map")
    df3.printSchema()
    println("DataFrame with map")
    df3.show()
    println("Count elements of each map in the column")
    df3.select($"id", size($"m").as("count")).show()

    // notice you get one column from the keys and one from the values
    println("Explode the map elements out into additional rows")
    df3.select($"id", explode($"m")).show()

    println("Select deep into the column with a Map")
    df3.select($"id", $"m.u").show()

    println("The column function getItem() seems to be the 'right' way")
    df3.select($"id", $"m".getItem("u")).show()
  }

}
