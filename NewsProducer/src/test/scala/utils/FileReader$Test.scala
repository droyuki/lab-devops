package utils

import kmlab.utils.ReadDir
import org.scalatest.FunSuite

/**
  * Created by WeiChen on 2015/11/29.
  */
class ReadDir$Test extends FunSuite {
   test("testReadDir") {
     val fileList = ReadDir.readDir("/Users/WeiChen/Desktop/fund_news_content")
     val l = fileList.length
     println(s"N: $l")
     fileList.foreach { file =>
       val source = scala.io.Source.fromFile(file)
       val str = source.getLines().mkString
       println("=========================")
       println(str)
     }
     assertResult(1)(1)
   }
 }
