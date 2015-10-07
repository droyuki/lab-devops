package bigboost.main

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/10/7.
 */
class TextSegmentTest extends SparkFunSuite {
  localTest("ANSJ Test") { sc =>
    val text = Array("台南市長賴清德上周在市議會說出台獨宣言，引發藍營痛批，台南市議員謝龍介5日質詢賴清德時，拿出賴清德擔任國大代表時高舉「台灣獨立萬歲」的照片諷刺，並加以抨擊，痛罵賴清德在糟蹋台灣人，但賴清德則回了一句話，讓謝一度無言")
    val input = sc.parallelize(text)
    val result = bigboost.main.TextSegmentation.ansj(input)
    println("[Start]")
    result.collect().foreach(a => a.foreach(t => println(t + ", ")))
    assertResult(1)(1)
  }

}
