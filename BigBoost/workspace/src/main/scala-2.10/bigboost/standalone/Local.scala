package bigboost.standalone

import java.util

import com.spreada.utils.chinese.ZHConverter
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.ansj.util.FilterModifWord

/**
 * Created by WeiChen on 2015/10/11.
 */
object Local {
  def zhConverterNoRdd(rdd: Array[String]): Array[String] = {
    rdd.map(text => ZHConverter.convert(text, ZHConverter.TRADITIONAL))
  }

  //return top N key words
  def topN(rdd: Array[String], top: Int): Array[String] = {
    val kwc = new KeyWordComputer(top)
    rdd.map { content =>
      val temp = kwc.computeArticleTfidf(content)
      val words = for (i <- Range(0, top)) yield temp.get(i).getName
      words.mkString("\\t")
    }
  }

  def ansj(rdd: Array[String], method: String = "NLP"): Array[String] = method match {
    case "To" =>
      if (rdd.length != 0)
        println("[Input RDD Count]" + rdd.length)
      rdd.map { x =>
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


    case "NLP" =>
      if (rdd.length != 0)
        println("[Input RDD Count]" + rdd.length)
      rdd.map { x =>
        val temp = NlpAnalysis.parse(x)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
  }
}
