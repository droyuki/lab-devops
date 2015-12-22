package com.kmlab.twitter

import com.kmlab.utils.CreateSparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * Created by WeiChen on 2015/12/16.
 */
object TwitterStream extends CreateSparkContext {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: Twitter Streaming <checkpointDirectory> <timeframe> [<filters>]")
      System.exit(1)
    }
    val Array(checkpointDirectory, timeframe) = args.take(2)
    val filters = args.takeRight(args.length - 2)

    //Twitter app key
    val consumerKey = "FqyZBWRTCLEyS8IgOqRwOwCh5"
    val consumerSecret = "ZWx4KgBpuJ9AaIARHpt59dM2K7fEpWgTDSGgY8YaevJINGONVA"
    val accessToken = "329689547-6BPOJ13eK8YFkHhjY3OTBbes8ZMwsV0reRrf0KKL"
    val accessTokenSecret = "GGtK134NLxXSnWBStQeQ5tPLoIWqnXor8Y4nHemgwDy6v"
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    def function2CreateContext(AppName: String, checkpointDirectory: String, timeframe: String): StreamingContext = {
      val ssc = createContext(AppName, checkpointDirectory, timeframe.toLong)
      val stream = TwitterUtils.createStream(ssc, None, filters)
      stream.foreachRDD{rdd =>
        //filter(status => status.getPlace.getName != "Taiwan").
        rdd.foreach{status =>
          //val words = status.getText.split(" ").mkString(", ")
          println(status.getPlace.getName)
          println(status.getText)
        }
      }
      //      val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
      //val hashTags = stream.flatMap(status => status.getText.split(" "))
//      stream.foreachRDD { (rdd, time) =>
//        rdd.map(t =>
//
//          t.getText
//        )
//        //      val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//        //        .map { case (topic, count) => (count, topic) }
//        //        .transform(_.sortByKey(false))
//
//        // Print popular hashtags
//        //      topCounts60.foreachRDD(rdd => {
//        //        val topList = rdd.take(10)
//        //        println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
//        //        topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
//        //      })
//      }
      ssc

    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        function2CreateContext("Twitter Streaming", checkpointDirectory, timeframe)
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

}
