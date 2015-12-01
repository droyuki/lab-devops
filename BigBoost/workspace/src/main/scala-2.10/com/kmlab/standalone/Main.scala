package com.kmlab.standalone

import java.io.{BufferedWriter, File, FileWriter}

import org.ansj.app.keyword.KeyWordComputer
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord

/**
 * Created by WeiChen on 2015/11/29.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val dirName = "/Users/WeiChen/Project/lab-devops/BigBoost/News"
    val fileList = ReadDir.readDir(dirName)
    val n = fileList.length
    println(s"Find $n files.")
    val rdd = fileList.map(
      scala.io.Source.fromFile(_).getLines().mkString
    ).toArray

    val file = new File("/Users/WeiChen/Desktop/news_TopWord.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val kwc = new KeyWordComputer(30)
    rdd.foreach { w =>
      //val temp = kwc.computeArticleTfidf(w)
      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/ntusd-positive.dic")
      val tempq = ToAnalysis.parse(w)
      FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e", "p", "a", "cc", "pba", "c", "pbei", "uyy", "ulian", "y", "o", "en")
      val temp = FilterModifWord.modifResult(tempq)

      val res = for{i <- 0 to temp.size()-1} yield{
        bw.write(temp.get(i).getName+",")
      }
      bw.write("\n")
    }
    bw.close()
  }
}
