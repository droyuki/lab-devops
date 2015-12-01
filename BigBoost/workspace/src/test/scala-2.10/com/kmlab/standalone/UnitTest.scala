package com.kmlab.standalone

import org.scalatest.FunSuite

/**
  * Created by WeiChen on 2015/11/22.
  */
class UnitTest extends FunSuite{
  val text = Array("人民銀行表示，鴻海的原物料買超明顯而且成本提高，全民消費支出全部變成很多")
  test("ANSJ NLP Test") {
    val result = com.kmlab.standalone.Local.ansj(text)
    result.foreach(println(_))
    assertResult(1)(1)
  }
  test("Conbination test"){
    val result = Local.combination(Local.ansj(text))
    result.foreach(println(_))
    assertResult(1)(1)
  }
}
