package com.kmlab.main

import java.util

import com.spreada.utils.chinese.ZHConverter
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.dic.LearnTool
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.ansj.util.FilterModifWord
import org.apache.spark.rdd.RDD

/**
 * Created by WeiChen on 2015/11/5.
 */
object ANSJ {
  def toSimplified(rdd: RDD[String]): RDD[String] = {
    val converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED)
    rdd.map(text => converter.convert(text))
  }

  def ansjTopN(rdd: RDD[String], top: Int): RDD[String] = {
    if (rdd.count() != 0)
      println("[Input RDD Count]" + rdd.count())
    val kwc = new KeyWordComputer(top)
    rdd.map { content =>
      val temp = kwc.computeArticleTfidf(content)
      val words = for (i <- Range(0, top)) yield temp.get(i).getName
      words.mkString("\\t")
    }
  }

  def ansj(rdd: RDD[String], method: Any = "To"): RDD[List[String]] = method match {
    case "To" =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        FilterModifWord.insertStopWord("多") ;
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e", "p", "a", "cc", "pba", "c", "pbei", "uyy", "ulian", "y", "o", "en")
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/userLibrary.dic")
        val temp = ToAnalysis.parse(x)
        val filter = FilterModifWord.modifResult(temp)
        filter.toArray.map(_.toString).toList //.split("/")).filter(_.nonEmpty).map(_ (0)).toList
      }
      procData

    case "NLP" =>
      if (rdd.count() != 0)
      println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val learnTool = new LearnTool()
        val temp = NlpAnalysis.parse(x, learnTool)
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e", "p", "q", "a", "cc", "pba", "c", "pbei", "uyy", "ulian", "y", "o", "en")
        val filter = FilterModifWord.modifResult(temp)
        filter.toArray.map(_.toString).toList
      }
      procData

    case n: Int =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val kwc = new KeyWordComputer(n)
      val procData = rdd.map { content =>
        val temp = kwc.computeArticleTfidf(content)
        temp.toArray.map(_.toString).toList
      }
      procData

    case _ =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val temp = ToAnalysis.parse(x)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        filter.toArray.map(_.toString).toList
        //此步骤将会只取分词，不附带词性
//        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
//        word.mkString("\t")
      }
      procData
  }

}
