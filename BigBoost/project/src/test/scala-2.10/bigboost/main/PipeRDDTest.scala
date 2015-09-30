package bigboost.main

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/9/27.
 */
class StreamingTest extends SparkFunSuite {
  localTest("Pipe to shell script Test"){ sc =>
    val input = sc.parallelize(Array("Ready to pipe", "This a sentence!!"))
    val scriptPath = " src/test/resources/testScript.sh"
    bigboost.main.PipeRDD.pipeData(input, scriptPath)
    assertResult(1)(1)
  }

  localTest("Pipe to R script Test"){ sc =>
    val input = sc.parallelize(Array("Ready to pipe", "This a sentence!!"))
    val scriptPath = " src/test/resources/testR.R"
    bigboost.main.PipeRDD.pipe2R(input, scriptPath)
    assertResult(1)(1)
  }

}
