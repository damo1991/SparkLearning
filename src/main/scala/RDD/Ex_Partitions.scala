package RDD

import java.util.logging.{Level, Logger}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by damodharraop on 9/24/2018.
  */
object Ex_Partitions {

  def analyze[T](r:RDD[T]):Unit={
    val partitions=r.glom()
    println(s"${partitions.count()} partitions")

    partitions.zipWithIndex().collect().foreach{
     case(a,i)=>{
       println(s"partition $i contains ${a.foldLeft("")((e,s)=>e+" "+s)}")
     }
    }


  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("aka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("partitions").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
  val numbers=sc.parallelize(1 to 100,4)
    println("Original RDD:")
    analyze(numbers)

    val some=numbers.filter(_<34)
    println("filter RDD:")
    analyze(some)
    val diff = numbers.subtract(some)
    println("the complement:")
    analyze(diff)
    println("it is a " + diff.getClass.getCanonicalName)

    // setting the number of partitions doesn't help (it was right anyway)
    val diffSamePart = numbers.subtract(some, 4)
    println("the complement (explicit but same number of partitions):")
    analyze(diffSamePart)

    // we can change the number but it also doesn't help
    // other methods such as intersection and groupBy allow this
    val diffMorePart = numbers.subtract(some, 6)
    println("the complement (different number of partitions):")
    analyze(diffMorePart)
    println("it is a " + diffMorePart.getClass.getCanonicalName)


    // but there IS a way to calculate the difference without
    // introducing communications
    def subtractFunc(wholeIter: Iterator[Int], partIter: Iterator[Int]) :
    Iterator[Int] = {
      val partSet = new mutable.HashSet[Int]()
      partSet ++= partIter
      wholeIter.filterNot(partSet.contains(_))
    }

    val diffOriginalPart = numbers.zipPartitions(some)(subtractFunc)
    println("complement with original partitioning")
    analyze(diffOriginalPart)
    println("it is a " + diffOriginalPart.getClass.getCanonicalName)

    val threePart = numbers.repartition(3)
    println("numbers in three partitions")
    analyze(threePart)
    println("it is a " + threePart.getClass.getCanonicalName)

    val twoPart = some.coalesce(2, true)
    println("subset in two partitions after a shuffle")
    analyze(twoPart)
    println("it is a " + twoPart.getClass.getCanonicalName)

    val twoPartNoShuffle = numbers.coalesce(2, false)
    println("subset in two partitions without a shuffle")
    analyze(twoPartNoShuffle)
    println("it is a " + twoPartNoShuffle.getClass.getCanonicalName)

    // a ShuffledRDD with interesting characteristics
    val groupedNumbers = numbers.groupBy(n => if (n % 2 == 0) "even" else "odd")
    println("numbers grouped into 'odd' and 'even'")
    analyze(groupedNumbers)
    println("it is a " + groupedNumbers.getClass.getCanonicalName)

    val pairs = sc.parallelize(for (x <- 1 to 6; y <- 1 to x) yield ("S" + x, y), 4)
    analyze(pairs)

    val rollup = pairs.foldByKey(0, 4)(_ + _)
    println("just rolling it up")
    analyze(rollup)
    println(s"it is a ${rollup.getClass.getCanonicalName}")
    def rollupFunc(i: Iterator[(String, Int)]) : Iterator[(String, Int)] = {
      val m = new mutable.HashMap[String, Int]()
      i.foreach {
        case (k, v) => if (m.contains(k)) m(k) = m(k) + v else m(k) = v
      }
      m.iterator
    }

    val inPlaceRollup = pairs.mapPartitions(rollupFunc, true)
    println("rolling it up really carefully")
    analyze(inPlaceRollup)

    println(s"it is a ${inPlaceRollup.getClass.getCanonicalName}")

  }

}
