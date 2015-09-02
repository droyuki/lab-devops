package bigboost.test

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/9/2.
 */
class mapLocalSQLTest extends SparkFunSuite {
  localTest("SparkTest") { sc =>
    val fakeRDD = sc.parallelize(Array("Fake RDD A", "Fae RDD B"))
    //bigboost.test.mapLocalSQL.printRDD(fakeRDD)
    //val actual = bigboost.test.mapLocalSQL.printRDD(fakeRDD)
    assertResult(2)(2)
  }
}
