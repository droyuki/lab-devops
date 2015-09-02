import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by WeiChen on 2015/9/2.
 */
case class Record(key: Int, value: String)

case class Model(rowkey: Option[String], title: Option[String], content: Option[String], dtime: Option[Long])

object mapLocalSQL {
  def main(args: Array[String]) {
    mappingLocalSQL()
  }

  def mappingLocalSQL() {
    val jarPaths = "target/scala-2.11/spark-hello_2.11-1.0.jar"
    val conf = new SparkConf().setMaster("spark://localhost:7077").setAppName("hdfs data count")
    conf.setJars(Seq(jarPaths))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
    df.registerTempTable("records")
    println("Result of SELECT *:")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)
    val count = sqlContext.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")
    sc.stop()
  }
}
