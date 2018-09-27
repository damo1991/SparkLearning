package RDD

import java.util.logging.{Level, Logger}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by damodharraop on 9/24/2018.
  */
object Ex2_Computations {

  private def showDep[T](r:RDD[T],depth:Int):Unit={
    println("".padTo(depth," ")+"RDD id= "+r.id)

    r.dependencies.foreach(dep=>{
      showDep(dep.rdd,depth+1)
    })

  }
  def showDep[T](r:RDD[T]): Unit ={
    showDep(r,0)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val conf=new SparkConf().setMaster("local[4]").setAppName("Computations")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val numbers=sc.parallelize(1 to 10,4)
    val bigger=numbers.map(x=>x*100)
    val biggerstill=bigger.map(n=>n+1)

   // println("Debug string for the RDD 'biggerstill'")
    //println(biggerstill.toDebugString)

    val s=biggerstill.reduce(_+_)
    println("IDs of the various RDDs")
    println("numbers: id=" + numbers.id)
    println("bigger: id=" + bigger.id)
    println("biggerStill: id=" + biggerstill.id)
    println("dependencies working back from RDD 'biggerStill'")
    showDep(biggerstill)
    val moreNumbers = bigger ++ biggerstill
    println("The RDD 'moreNumbers' has mroe complex dependencies")
    println(moreNumbers.toDebugString)
    println("moreNumbers: id=" + moreNumbers.id)
    showDep(moreNumbers)

    moreNumbers.cache()
    // things in cache can be lost so dependency tree is not discarded
    println("cached it: the dependencies don't change")
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)
    println("has RDD 'moreNumbers' been checkpointed? : " + moreNumbers.isCheckpointed)
    // set moreNumbers up to be checkpointed
    sc.setCheckpointDir("/tmp/sparkcps")
    moreNumbers.checkpoint()
    // it will only happen after we force the values to be computed
    println("NOW has it been checkpointed? : " + moreNumbers.isCheckpointed)
    moreNumbers.count()
    println("NOW has it been checkpointed? : " + moreNumbers.isCheckpointed)
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)
  }

}
