package bigboost.test

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/9/2.
 */
class mapLocalSQLTest extends SparkFunSuite {
  localTest("SparkTest") { sc =>
    val fakeRDD = sc.parallelize(Array("Fake RDD A", "Fae RDD B"))
    bigboost.test.mapLocalSQL.printRDD(fakeRDD)
    val actual = bigboost.test.mapLocalSQL.printRDD(fakeRDD)
    assertResult(2)(actual)
  }

  localTest("AlphabetCountTest") { sc =>
    val fakeData = Array("Apache Kafka is a popular distributed message broker designed to handle large volumes of real-time data efficiently. \n\nA Kafka cluster is not only highly scalable and fault-tolerant, but it also has a much higher throughput compared to other message brokers such as ActiveMQ and RabbitMQ. Though it is generally used as a pub/sub messaging system, a lot of organizations also use it for log aggregation because it offers persistent storage for published messages.\n\nIn this tutorial, you will learn how to install and use Apache Kafka 0.8.2.1 on Ubuntu 14.04.To follow along, you will need:\n\nUbuntu 14.04 Droplet")
    val fakeRDD = sc.parallelize(fakeData)
    val actual = bigboost.test.mapLocalSQL.alphabetCount(fakeRDD)._1
    assertResult(516)(actual)
  }
}
