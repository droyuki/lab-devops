package com.kmlab.main

import java.io.{File, PrintWriter}

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, Word2Vec}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by WeiChen on 2015/12/11.
 */

object CooccurrenceAnalysis {
  private var dfsDirPath: String = ""

  private def printUsage(): Unit = {
    val usage: String = "Co-occurrence Analysis\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"
    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 1) {
      printUsage()
      System.exit(1)
    }
    dfsDirPath = args(0)
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    val conf = new SparkConf().setAppName("Co-occurrence Analysis")
    val sc = new SparkContext(conf)
    println("Reading file from HDFS")
    val dfsFilename = dfsDirPath + "/news_word.txt"
    val fileRDD = sc.textFile(dfsFilename)
    val pmi = genPmi(fileRDD)
    val sortedPMI = sortByPmi(pmi, 100)

    val result = cooccurrence(fileRDD, 5)
    val top100 = sortByCount(result, 100)

    val output = dfsDirPath + "/model/"
    val fortest = "hdfs://bigboost-spark:9000/testData/model/"
    genWord2Vec(fileRDD, fortest)
    //val m = loadWord2Vec(sc, fortest)
    //m.findSynonyms("成長", 3)
    sc.stop()
  }

  def loadWord2Vec(sc: SparkContext, path2Model: String) = {
    import org.apache.spark.mllib.feature.Word2VecModel
    Word2VecModel.load(sc, path2Model)
  }


  def genWord2Vec(fileRDD: RDD[String], output: String): Unit = {
    import org.apache.spark.mllib.feature.Word2Vec
    val sc = fileRDD.sparkContext
    val input = fileRDD.map(_.split(" "))
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input.map(_.toSeq))
    val vMap = model.getVectors
    val vArray = vMap.toArray.map(p => p._2)
    val v = vMap("財經")
    model.findSynonyms("政治", 5)
    model.save(fileRDD.sparkContext, output)
    val data = sc.parallelize(vArray)
    import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
    import org.apache.spark.mllib.linalg.Vectors
    val parsedData = data.map(s => Vectors.dense(s.map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 3
    val numIterations = 100
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val vPair = vMap.toArray.map(p => (p._1, p._2))
    val result = for{p <- vPair} yield {
      (p._1,clusters.predict(Vectors.dense(p._2.map(_.toDouble))))
    }
    result.sortBy(_._2)
    //result.filter(_._2 == 0).foreach(p => writer.write(p+"\n"))

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    clusters.save(sc, "myModelPath")
    val sameModel = KMeansModel.load(sc, "myModelPath")
  }

  def genWord2Vec_DF(fileRDD: RDD[String]): Unit = {
    val sc = fileRDD.sparkContext
    val sqlContext = new SQLContext(sc)
    val rdd = fileRDD.map(_.split(" ")).map(Tuple1.apply).collect
    val documentDF = sqlContext.createDataFrame(rdd).toDF("text").repartition(50)
    // Learn a mapping from words to Vectors.
    val size = fileRDD.count().toInt
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(size).setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }

  def genTFIDF(fileRDD: RDD[String]): Unit = {
    val sc = fileRDD.sparkContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val text = fileRDD.toDF("sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(text)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val featurizedData = hashingTF.transform(wordsData)
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
  }


  def cooccurrence(rdd: RDD[String], windowSize: Int): RDD[((String, String), Int)] = {
    val sc = rdd.sparkContext
    val fileRDD = sc.makeRDD(rdd.map(_.split(" ")).reduce((x, y) => x ++ y))
    val rddFunc = new RDDFunctions(fileRDD)
    val res = rddFunc.sliding(windowSize).map { arr =>
      //      val tuples = for{index <- arr.indices} yield {
      //        (arr(0), arr(index), 1)
      //      }
      ((arr(0), arr(windowSize - 1)), 1)
      //tuples
    }
    res.reduceByKey((x, y) => x + y)
  }

  def genPmi(rdd: RDD[String]): RDD[((String, String), Double)] = {
    val sc = rdd.sparkContext
    val fileRDD = sc.parallelize(rdd.map(_.split(" ")).reduce((x, y) => x ++ y)).map((_, 1))
    val allTerms = fileRDD.reduceByKey((x, y) => x + y)
    val termCountMap = Map(allTerms.collect(): _*)
    val totalWordNum = fileRDD.count().toDouble

    val rddFunc = new RDDFunctions(fileRDD)
    val pairs = rddFunc.sliding(2).map(arr =>
      (((arr(0)._1, termCountMap.getOrElse(arr(0)._1, 1)), (arr(1)._1, termCountMap.getOrElse(arr(1)._1, 1))), 1)
    ).reduceByKey((x, y) => x + y)

    sc.broadcast(termCountMap)
    sc.broadcast(totalWordNum)

    //pair: ( ((String, Int),(String, Int)) , Int)
    val pmi = pairs.map { pair =>
      val x = pair._1._1
      val y = pair._1._2
      val pXandY = pair._2.toDouble / totalWordNum
      val pX = x._2.toDouble / totalWordNum
      val pY = y._2.toDouble / totalWordNum
      val pmi = Math.log(pXandY / (pX * pY))
      ((x._1, y._1), pmi)
    }
    pmi
  }


  def sortByCount(rdd: RDD[((String, String), Int)], topN: Int): Array[((String, String), Int)] = {
    rdd.top(topN)(new Ordering[((String, String), Int)]() {
      def compare(x: ((String, String), Int), y: ((String, String), Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })
  }

  def sortByPmi(rdd: RDD[((String, String), Double)], topN: Int): Array[((String, String), Double)] = {
    rdd.top(topN)(new Ordering[((String, String), Double)]() {
      def compare(x: ((String, String), Double), y: ((String, String), Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })
  }

  def fpGrowth(sc: SparkContext): Unit = {
    val data = sc.textFile("hdfs://bigboost-spark:9000/testData/news_wordSet_by_article.txt")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10)
    val model = fpg.run(transactions)
    val writer = new PrintWriter(new File("/fpGrowth.txt"))
    model.freqItemsets.collect().foreach { itemset =>
      val c = itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + "\n"
      writer.write(c)
    }
    writer.close

    val minConfidence = 0.85
    val writer2 = new PrintWriter(new File("/rule.txt"))
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      val c = rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence + "\n"
      writer2.write(c)
    }
    writer2.close
  }

}