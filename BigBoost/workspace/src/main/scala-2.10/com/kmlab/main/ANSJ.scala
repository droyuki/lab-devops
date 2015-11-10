package com.kmlab.main

import java.util

import com.spreada.utils.chinese.ZHConverter
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.dic.LearnTool
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

  def topN(rdd: RDD[String], top: Int): RDD[String] = {
    if (rdd.count() != 0)
      println("[Input RDD Count]" + rdd.count())
    val kwc = new KeyWordComputer(top)
    rdd.map { content =>
      val temp = kwc.computeArticleTfidf(content)
      val words = for (i <- Range(0, top)) yield temp.get(i).getName
      words.mkString("\\t")
    }
  }

  def ansj(rdd: RDD[String], method: Any = "NLP"): RDD[String] = method match {
    case "NLP" =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val learnTool = new LearnTool();
        val temp = NlpAnalysis.parse(x, learnTool)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData

    case "To" =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val temp = ToAnalysis.parse(x)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData

    case n: Int =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val kwc = new KeyWordComputer(n)
      val procData = rdd.map { content =>
        val temp = kwc.computeArticleTfidf(content)
        val words = for (i <- Range(0, n)) yield temp.get(i).getName
        words.mkString("\\t")
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
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData
  }

}
