package com.kmlab.standalone

import java.util

import com.spreada.utils.chinese.ZHConverter
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.dic.LearnTool
import org.ansj.library.UserDefineLibrary
import org.ansj.recognition.NatureRecognition
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.ansj.util.FilterModifWord

/**
  * Created by WeiChen on 2015/10/11.
  */
object Local {
  def combination(rdd: Array[List[String]]): Array[List[String]] = {
    rdd.map { list =>
      val returnList = for {i <- 0 to list.length - 2} yield {
        val term = list(i).split("/")
        val nextTerm = list(i + 1).split("/")
        val newWord = if ((term(1) == "n" && nextTerm(1) == "v")
          || (term(1) == "vf" && nextTerm(1) == "vi")
          || (term(1) == "n" && nextTerm(1) == "n")
          || (term(1) == "vn" && nextTerm(1) == "n")
          || (term(1) == "b" && nextTerm(1).matches("n"))
          || (term(1) == "a" && nextTerm(1).matches("n"))
          || (term(1) == "v" && nextTerm(1).matches("a"))) {
          term(0) + nextTerm(0)
        } else {
          "NULL"
        }
        newWord
      }
      returnList.toList.filterNot(_ == "NULL")
    }
  }

  def zhConverterNoRdd(rdd: Array[String]): Array[String] = {
    rdd.map(text => ZHConverter.convert(text, ZHConverter.TRADITIONAL))
  }

  //return top N key words
  def topN(rdd: Array[String], top: Int): Array[String] = {
    val kwc = new KeyWordComputer(top)
    rdd.map { content =>
      val temp = kwc.computeArticleTfidf(content)
      val words = for(i <- 0 to top-1)yield {
        temp.get(i).getName
      }
      words.mkString("\t")
    }
  }

  def ansj(rdd: Array[String], method: String = "NLP"): Array[List[String]] = method match {
    case "To" =>
      if (rdd.length != 0)
        println("[Input RDD Count]" + rdd.length)
      rdd.map { x =>
        //FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //FilterModifWord.insertStopWord("多");
        //FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e", "p", "a", "cc", "pba", "c", "pbei", "uyy", "ulian", "y", "o", "en")
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/userLibrary.dic")
        val temp = ToAnalysis.parse(x)
        val filter = FilterModifWord.modifResult(temp)
        filter.toArray.map(_.toString).toList //.split("/")).filter(_.nonEmpty).map(_ (0)).toList
      }

    case "NLP" =>
      if (rdd.length != 0)
        println("[Input RDD Count]" + rdd.length)
      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/ntusd-positive.dic")
      //UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/userLibrary.dic")
      FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
      FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
      //FilterModifWord.insertStopNatures(null,"nis","nnt","nr","nr","nr","nrj","nrf","ns","nsf","nt","nz","nl","ng","nw","t","tg","s","f","v","vd","vn","vshi","vyou","vf","vx","vi","vl","vg","a","ad","an","ag","al","b","bl","z","r","rr","rz","rzt","rzs","rzv","ry","ryt","rys","ryv","rg","m","mq","q","qv","qt","d","p","pba","pbei","c","cc","u","uzhe","ule","uguo","ude","usuo","udeng","uyy","udh","uls","uzhi","ulian","e","ydeleteyg","o","h","k","x","xx","xu","w","wkz","wky","wyz","wyy","wj","ww","wt","wd","wf","wn","wm","ws","wp","wb","wh","en")
      val learnTool = new LearnTool()
      rdd.map { x =>
        val temp = NlpAnalysis.parse(x, learnTool)
        new NatureRecognition(temp).recognition()
        val filter = FilterModifWord.modifResult(temp)
        filter.toArray.map(_.toString).toList
      }
  }
}