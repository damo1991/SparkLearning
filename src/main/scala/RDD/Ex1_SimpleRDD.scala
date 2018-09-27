package RDD

/**
  * Created by damodharraop on 9/20/2018.
  */
import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
object Ex1_SimpleRDD {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val conf=new SparkConf().setMaster("local[*]").setAppName("Ex1_SimpleRDD")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val number=1 to 10

    val numberRDD=sc.parallelize(number,4)
    numberRDD.foreach(println)

    val stillAnRDD=numberRDD.map(n=>n.toDouble/10)

    val nowAnArray=stillAnRDD.collect()
    println("printing an array in :")
    nowAnArray.foreach(println)

    val partition=stillAnRDD.glom()
println(s"we have ${partition.count} partitions")

    partition.foreach(a=>{
      println(s"partition contains:${a.foldLeft("")((s,e)=>s+" " +e)}")
    })

  }

}
