package example

/**
 * Created by WeiChen on 2015/9/7.
 */

import org.scalatest.FunSuite

class LocalTestSuite extends FunSuite {
  test("CallPythonTest") {
    val expected = (2, "Hello, world")
    val actual = example.callPython.call()
    assertResult(expected)(actual)
  }
}
