package kmlab.utils

import java.io.File


/**
  * Created by WeiChen on 2015/11/28.
  */
object ReadDir {
  def readDir(dirName: String): List[File] = {
    val d = new File(dirName)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.getName.endsWith(".txt")).toList
    } else {
      List[File]()
    }
  }
}
