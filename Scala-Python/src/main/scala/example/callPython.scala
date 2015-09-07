package example

import jep.Jep

object callPython {
  def main(args: Array[String]) = {
    val res = call()
    println(res._1, res._2)
  }

  def call(): (AnyRef, AnyRef) = {
    val jep = new Jep()
    jep.runScript("src/main/python/callMe.py")

    val res1 = {
      val i = 1
      val fn = "fn1"
      val r1 = jep.invoke(fn, i.asInstanceOf[AnyRef])
      println(s"$fn($i) = $r1")
      r1
    }

    val res2 = {
      val s = "world"
      val fn = "fn2"
      val r2 = jep.invoke(fn, s.asInstanceOf[AnyRef])
      println( s"""$fn("$s") = $r2""")
      r2
    }
    (res1, res2)
  }
}
