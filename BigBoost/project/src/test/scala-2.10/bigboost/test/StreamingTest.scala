package bigboost.test

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/9/27.
 */
class StreamingTest extends SparkFunSuite {
  localTest("PipeRDDTest"){ sc =>
    val testData = Array("This is a sentence.", "This a sentence!!")
    val scriptPath = " src/test/resources/testScript.sh"
    val input = sc.parallelize(testData)
    bigboost.test.PipeRDD.pipeData(input, 1, scriptPath)
    assertResult(1)(1)
  }
}
