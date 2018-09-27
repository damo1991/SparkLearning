package RDD

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by damodharraop on 9/24/2018.
  */
object CombiningRDD {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val conf=new SparkConf().setAppName("CombiningRDD").setMaster("local[4]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
     val letters=sc.parallelize('a' to 'z',8)
    val vowels=sc.parallelize(Seq('a','e','i','o','u'),4)
    val constants=letters.subtract(vowels)
    println(s"number of constants are ${constants.count()}")

    val earlyLetters=sc.parallelize('a' to 'l')
    val earlyVowels=earlyLetters.intersection(vowels)
    earlyVowels.foreach(println)

    val indexed=letters.zipWithIndex()
    println("indexed letters")

    indexed.foreach {
      case (c, i) => println(s"$i : $c")
    }

  }

}
