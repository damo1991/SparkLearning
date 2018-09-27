package RDD

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by damodharraop on 9/24/2018.
  */
object MoreOperationsOnRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val conf=new SparkConf().setAppName("more operations on rdd").setMaster("local[4]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val letters=sc.parallelize('a' to 'z',8)
    val vowels=Seq('a','e','i','o','u')
    val constants=letters.filter(c=> !vowels.contains(c))
    //constants.foreach(println)
    val constantsAsDigits=letters collect{
      case c:Char if !vowels.contains(c)=>c.asDigit
    }
//    constantsAsDigits.foreach(println)
    val words=sc.parallelize(Seq("Hello","World"))
    val chars=words.flatMap(w=>w.iterator)

    println(chars.map(c=>c.toString).reduce((s1,s2)=>s1+" "+s2))

    //groupby
    val numbers=sc.parallelize(1 to 10,4)
    val modThreeGroups=numbers.groupBy(_%3)

    modThreeGroups foreach{
      case (m,vals)=>println(s"mod 3 = $m count= ${vals.count(_=>true)}")
    }

  val mods=modThreeGroups.collect({
    case(m,vals)=>vals.count(_=>true)
  }).countByValue()
    mods.foreach(println)

    println(mods.get(3))
    println(mods.get(7))







  }

}
