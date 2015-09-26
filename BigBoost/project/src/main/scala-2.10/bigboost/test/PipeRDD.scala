package bigboost.test

import org.apache.spark.rdd.RDD

/**
 * Created by WeiChen on 2015/9/27.
 */
object PipeRDD extends SparkContext{

  def pipeData(rdd: RDD[String], timeFrame: Long, scriptPath: String): RDD[String] = {
    println("[RDD Count]"+rdd.count())
    val dataProc = rdd.pipe(scriptPath)
    dataProc.collect().foreach(t => println("[Proc Data]" + t))
    dataProc
  }

  def printData(rdd:RDD[String]): Unit ={
    rdd.collect().foreach(t => println("[Raw Data]"+ t))
  }

}
