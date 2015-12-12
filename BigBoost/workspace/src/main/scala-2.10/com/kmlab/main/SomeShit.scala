package com.kmlab.main

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by WeiChen on 2015/12/12.
 */
object SomeShit {
  private var dfsDirPath: String = ""
  def someShit(sc:SparkContext): Unit ={
    val sqlContext = new SQLContext(sc)
    println("Reading file from HDFS")
    val dfsFilename = dfsDirPath + "/news_word.txt"
    val fileRDD = sc.textFile(dfsFilename)
    // val fileRDD = sc.textFile("hdfs://bigboost-spark:9000/testData/news_word.txt")
    val input = fileRDD.map(_.split(" "))
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input.map(_.toSeq))

    val allTerms = sc.makeRDD(input.reduce((x, y) => x ++ y))
    val distinctSet = sc.makeRDD(fileRDD.map(_.split(" ").distinct).reduce((x, y) => x ++ y))
    val distinctJoinSet = distinctSet.cartesian(distinctSet).map(data => (data._1, data._2, 0))

    val indexedAllTerms = allTerms.zipWithIndex().map { case (v, k) => (k, v) }
    val indexedDistinctSet = distinctSet.zipWithIndex().map { case (v, k) => (k, v) }
    val indexedDistinctJoinSet = distinctJoinSet.zipWithIndex().map { case (v, k) => (k, v) }
    import sqlContext.implicits._
    val df = distinctJoinSet.toDF("word1", "word2", "count")


    val indexedVectorInModel = distinctSet.flatMap { term =>
      try {
        Some(model.transform(term))
      } catch {
        case e: IllegalStateException =>
          None
      }
    }.zipWithIndex().map { case (v, k) => (k, v) }

    def genConnection() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://bigboost-mysql:3306/DATA", "root", "")
    }
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "")
    df.write.jdbc("jdbc:mysql://bigboost-mysql:3306/DATA", "table1", prop)
    val jdbcRDD = new JdbcRDD(sc, genConnection,
      "SELECT content FROM mysqltest WHERE ID >= ? AND ID <= ?", 1, 100, 3,
      resultSet => resultSet.getString(1)
    ).cache()

    print(jdbcRDD.filter(_.contains("success")).count())


    //dataFrame.registerTempTable("cooccurrence")
    val indexKey = allTerms.zipWithIndex().map { case (k, v) => (v, k) }.cache()
    val boundary = indexKey.count()
    indexKey.foreach { case (index, word) =>
      if (index < boundary) {
        val pair = (word, indexKey.lookup(index + 1).toString())
        println(pair._1 + ", " + pair._2)

      }
    }
  }


}
