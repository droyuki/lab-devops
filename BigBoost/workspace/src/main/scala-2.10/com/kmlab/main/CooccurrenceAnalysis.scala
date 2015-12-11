package com.kmlab.main

import java.io.File
import java.sql.DriverManager

import breeze.numerics.sqrt
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source._

/**
 * Created by WeiChen on 2015/12/9.
 */
/**
 * Simple test for reading and writing to a distributed
 * file system.  This example does the following:
 *
 * 1. Reads local file
 * 2. Computes word count on local file
 * 3. Writes local file to a DFS
 * 4. Reads the file back from the DFS
 * 5. Computes word count on the file using Spark
 * 6. Compares the word count results
 */
object CooccurrenceAnalysis {
  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""
  private val NPARAMS = 2

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  private def printUsage(): Unit = {
    val usage: String = "DFS Read-Write Test\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    localFilePath = new File(args(i))
    if (!localFilePath.exists) {
      System.err.println("Given path (" + args(i) + ") does not exist.\n")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println("Given path (" + args(i) + ") is not a file.\n")
      printUsage()
      System.exit(1)
    }

    i += 1
    dfsDirPath = args(i)
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    println("Creating SparkConf")
    val conf = new SparkConf().setAppName("Co occurrence Analysis")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println("Reading file from HDFS")
    val dfsFilename = dfsDirPath + "/news_word.txt"
    val fileRDD = sc.textFile(dfsFilename)

    val input = fileRDD.map(_.split(" "))
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input.map(_.toSeq))

    val allTerms = sc.makeRDD(input.reduce((x, y) => x ++ y))
    val indexedAllTerms = allTerms.zipWithIndex().map { case (v, k) => (k, v) }

    val distinctSet = sc.makeRDD(fileRDD.map(_.split(" ").distinct).reduce((x, y) => x ++ y))
    val indexedDistinctSet = distinctSet.zipWithIndex().map { case (v, k) => (k, v) }

    val distinctJoinSet = distinctSet.cartesian(distinctSet).map(data => (data._1, data._2, 0))
    val indexedDistinctJoinSet = distinctJoinSet.zipWithIndex().map { case (v, k) => (k, v) }


    val indexedVectorInModel = distinctSet.flatMap { term =>
      try {
        Some(model.transform(term))
      } catch {
        case e: IllegalStateException =>
          None
      }
    }.zipWithIndex().map { case (v, k) => (k, v) }

    val jdbcRDD = new JdbcRDD( sc, () => {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          DriverManager.getConnection("jdbc:mysql://bigboost-mysql:3306/DATA", "root", "")
        },
        "SELECT content FROM mysqltest WHERE ID >= ? AND ID <= ?", 1, 100, 3,
      resultSet => resultSet.getString(1)
    ).cache()

    print(jdbcRDD.filter(_.contains("success")).count())


    //    indexedDistinctJoinSet.foreach( case(word1,word2,index)
    //        model.transform(_)
    //      )
    //dataFrame.registerTempTable("cooccurrence")
    val indexKey = allTerms.zipWithIndex().map { case (k, v) => (v, k) }.cache()
    val boundary = indexKey.count()
    indexKey.foreach { case (index, word) =>
      if (index < boundary) {
        val pair = (word, indexKey.lookup(index + 1).toString())
        println(pair._1 + ", " + pair._2)

      }
    }
    sc.stop()
  }

  def Cooccurrence(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备
    val user_rdd2 = user_rdd.map(f => (f._1, f._2)).sortByKey()
    user_rdd2.cache

    //  1 (用户：物品)笛卡尔积 (用户：物品) =>物品:物品组合
    val user_rdd3 = user_rdd2.cartesian(user_rdd2)
    val user_rdd4 = user_rdd3.map(data => (data._2, 1))

    //  2 物品:物品:频次
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    //  3 对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)

    //  4 非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算同现相似度（物品1，物品2，同现频次）
    val user_rdd8 = user_rdd7.map(f =>
      (f._1._1, (f._1._1, f._1._2, f._2))
    ).join(
        user_rdd6.map(f => (f._1._1, f._2))
      )
    val user_rdd9 = user_rdd8.map(f =>
      (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2))
    )
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, f._3 / sqrt(f._4 * f._5)))

    //   6结果返回
    user_rdd12
  }

}


