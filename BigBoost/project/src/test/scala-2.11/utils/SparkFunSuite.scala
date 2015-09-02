package utils
/**
 * Created by WeiChen on 2015/9/2.
 */
import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class SparkFunSuite extends FunSuite {
  def localTest(name : String)(f : SparkContext => Unit) : Unit = {
    this.test(name) {
      val conf = new SparkConf()
        .setAppName(name)
        .setMaster("local")
        .set("spark.default.parallelism", "1")
      val sc = new SparkContext(conf)
      try {
        f(sc)
      } finally {
        sc.stop()
      }
    }
  }
}
