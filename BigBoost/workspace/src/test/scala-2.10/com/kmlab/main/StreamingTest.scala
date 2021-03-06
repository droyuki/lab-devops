package com.kmlab.main

import com.kmlab.main.PipeRDD
import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/9/27.
 */
class StreamingTest extends SparkFunSuite {
  localTest("Pipe to shell script Test"){ sc =>
    val input = sc.parallelize(Array("Ready to pipe", "This a sentence!!"))
    val scriptPath = " src/test/resources/testScript.sh"
    PipeRDD.pipeData(input, scriptPath)
    assertResult(1)(1)
  }

}
