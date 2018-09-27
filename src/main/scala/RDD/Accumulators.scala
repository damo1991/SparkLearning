package RDD

import java.util
import java.util.Collections

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by damodharraop on 9/25/2018.
  */
object Accumulators {


  class StringSetAccumulator extends AccumulatorV2[String,java.util.Set[String]] {
    private val _set=Collections.synchronizedSet(new util.HashSet[String]())

    override def isZero: Boolean = _set.isEmpty

    override def copy(): AccumulatorV2[String, util.Set[String]] = {
      val newAcc = new StringSetAccumulator()
      newAcc._set.addAll(_set)
      newAcc
    }

    override def reset(): Unit = {
      _set.clear()
    }

    override def add(v: String): Unit = _set.add(v)

    override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = _set.addAll(other.value)

    override def value: util.Set[String] = _set
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc=new SparkContext(conf)

    val words=sc.parallelize(Seq("Fred", "Bob", "Francis",
      "James", "Frederick", "Frank", "Joseph"), 4)

    val count=sc.longAccumulator

    words.filter(_.startsWith("F")).foreach(n=>count.add(1))


    println(s"printing no.of peoples start with F ${count.value}")

    val names = new StringSetAccumulator
    sc.register(names)

    println("*** using a set accumulator")
    words.filter(_.startsWith("F")).foreach(names.add)
    println("All the names starting with 'F' are a set")

    println(s"names start with F are ${names.value}")

  }




}
