package com.kmlab.main

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/11/5.
 */
class TFIDFSparkTest extends SparkFunSuite {
  localTest("TFIDF Test") { sc =>
    val res = TextSegmentation.calculateTFIDF(sc, "src/test/resources/testChinese.txt")
    res.collect().foreach(t => println("[TF]" + t))
    assertResult(1)(1)
  }
}
