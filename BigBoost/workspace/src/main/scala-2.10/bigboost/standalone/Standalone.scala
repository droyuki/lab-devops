package bigboost.standalone

import com.spreada.utils.chinese.ZHConverter
import org.ansj.dic.LearnTool
import org.ansj.splitWord.analysis.NlpAnalysis

/**
 * Created by WeiChen on 2015/10/11.
 */
object Standalone {
  def zhConverterNoRdd(rdd:Array[String]):Array[String] ={
    rdd.map(text => ZHConverter.convert(text, ZHConverter.TRADITIONAL))
  }
  def ansjNoRdd(input:String): Array[String]={
    val learnTool = new LearnTool()
    var temp = NlpAnalysis.parse(input, learnTool)
    temp = NlpAnalysis.parse(input, learnTool)
    val word = for (i <- Range(0, temp.size())) yield temp.get(i).getName
    word.toArray
  }
}
