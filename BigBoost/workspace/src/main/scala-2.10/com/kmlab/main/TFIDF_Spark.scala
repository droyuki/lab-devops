package com.kmlab.main

import java.io._
import java.util

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.Array.canBuildFrom
import scala.io.Source

/**
  * Created by WeiChen on 2015/11/5.
  */
object TFIDF_Spark {
  final val idfPath = "/idf.cache"
  final val customIDFPath = "idf.cache"
  final val defaultIDF = Math.PI
  val stopwords = List("resource/engStopwords.txt", "resource/zhStopwords.txt").flatMap(Source.fromFile(_).getLines().map(_.trim))

  def getKeywords(content: RDD[String], topN: Int = 10, corpus: String, sc: SparkContext): RDD[List[(String, Double)]] = {
    val terms = content.map { rdd =>
      FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
      FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/userLibrary.dic")
      ToAnalysis.parse(rdd).toArray.map(_.toString.split("/")).filter(_.nonEmpty).map(_ (0)).toList
    }

    //tf:RDD[Map(word, freq)]
    val tf = terms.map { list =>
      TF(list.filter(word => word.length >= 2 && !stopwords.contains(word)))
    }
    val idf = IDF()
    val tfidf = tf.map { tfMap =>
      tfMap.map { item =>
        // word -> frequency
        item._1 -> item._2 * idf.getOrElse(item._1, 3.76)
      }.toList
    }
    tfidf.map(_.sortBy(_._2).reverse.take(topN))
  }

  /**
    * Construct your own IDF with a corpus.
    * @param corpusPath the corpusPath must be a directory containing a huge number of documents.
    */


  def constructCorpus(corpusPath: String, sc: SparkContext) = {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    assert(fs.exists(new org.apache.hadoop.fs.Path(corpusPath)))
    //hdfs path. eg: "hdfs://some-directory/*"

    //(filename, content)
    val allFiles = sc.wholeTextFiles(corpusPath)
    val fileCount = allFiles.count()
    val corpus = allFiles.map(_._2).flatMap { file =>
      segment(file.mkString).distinct
    }.groupBy(x => x).map { case (word, list) =>
      word -> Math.log(fileCount * 1.0 / list.size + 1)
    }

    // serialize the corpus
    val writer = new ObjectOutputStream(
      new FileOutputStream(
        new File(customIDFPath)))
    writer.writeObject(corpus)
    writer.close()
  }

  /**
    * Not a short symbol which length less that 2, not a stopword, not a number.
    * @param terms list of words
    * @return
    */
  private def filterTrivialWord(terms: List[String]) = {
    terms.filter { word =>
      word.length >= 2 && !stopwords.contains(word) && !isNumber(word)
    }
  }

  private def isNumber(term: String): Boolean = {
    term.forall { x =>
      ('0' <= x && x <= '9') || x == '.'
    }
  }

  /**
    * article segmentation.
    * @param content the article to be segmented.
    * @return terms segmented.
    */

  private def segment(content: String): List[String] = {
    ToAnalysis.parse(content)
      .toArray
      .map(_.toString.split("/"))
      .filter(_.length >= 2)
      .map(_ (0))
      .toList
  }


  private def IDF(): Map[String, Double] = {
    var cacheIS: InputStream = null
    // prefer user-defined idf
    if (new File(customIDFPath).exists)
      cacheIS = new FileInputStream(new File(customIDFPath))
    else
      cacheIS = getClass.getResourceAsStream(idfPath)
    // deserialize
    val reader = new ObjectInputStream(cacheIS)
    val corpus = reader.readObject.asInstanceOf[Map[String, Double]]
    reader.close()
    corpus
  }

  private def TF(article: List[String]) = {
    val sum = article.length
    // word count
    article.groupBy(x => x).map { case (word, list) =>
      word -> list.length.toDouble / sum
    }
  }

}
