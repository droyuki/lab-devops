package bigboost.main

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by WeiChen on 2015/9/27.
 */
class SparkContext {
  def createContext(appName: String, checkpointDirectory: String, timeFrame: Long): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc =  new StreamingContext(sparkConf, Seconds(timeFrame))
    ssc.checkpoint(checkpointDirectory)
    ssc
  }
}
