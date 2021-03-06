package com.kmlab.main

import com.kmlab.main.ANSJ._
import utils.SparkFunSuite

/**
  * Created by WeiChen on 2015/11/10.
  */
class ReadFileTest extends SparkFunSuite {
  localTest("Read from disk") { sc =>
    //val text = sc.textFile("file:///Users/WeiChen/Desktop/TestArticle/2.txt")
    val rdd = sc.parallelize(Array("台指期週二下跌96點收在8532點。價差方面，台指期轉為逆價差縮至4.9點，電子期轉為正價差至0.01點，金融期轉為正價差至0.37點，摩台期逆價差縮至0.28點。由籌碼面來觀察，三大法人於現貨市場賣超131.21億；而在台指期淨部位方面，三大法人多單減碼1478口，淨多單降至8799口，其中外資多單減碼1800口，淨多單降至14026口；另外十大交易人中的特定法人，全月份台指期多單加碼681口，淨多單增至12388口，顯示目前法人與大額交易人對台股短線續彈預期持續未變。期貨行情走勢方面，歐美股市開始反應美國升息恐慌大跌，造成週二台指期早盤以下跌47點開出，且現貨開盤也以下跌開出且在10分鐘內跌幅擴大至百點以上，造成期貨同步爆出近萬口大量並急跌近40點以上，雖台股急跌後部分權值股止穩拉抬指數收斂跌幅，但盤中隨著韓股與港股跌幅加重，11點過後仍震盪走低未能以紅K坐收，最終期貨以大跌96點作收。技術面觀察，週二大盤開低走低以帶中黑K吐回11月全部漲幅，所幸成交量能萎縮至861億水準，短線量價結構不利多方續航。而以日線型態來看，目前指數日K連四黑且今日出現島狀反轉缺口，且短期均線轉為彎頭向下，而多項技術指標出現轉弱訊號，趨勢仍為高檔震盪偏空格局。短線維持高檔震盪偏空操作上建議可在月線與季線間來回操作並請嚴設停損。選擇權從成交量來觀察，11月份買權集中在8700點，賣權集中在8500點。未平倉量部份，買權大量區至9000點，而賣權大量區至8400點。全月份未平倉量put/call ratio由0.92降至0.86，顯示選擇權結構維持中性發展。11月買權平均隱含波動率由16.46%降至16.32%，賣權平均隱含波動率由19.36%降至19.25%，買賣權同降，週二指數開低走低高連兩日黑K跌破月線，短線多方氣勢受阻，操作上建議持續以買權空頭價差策略因應。"))
    val proc = ansj(rdd) //RDD[List[String]]
      .map(TextSegmentation.tfTopN(_, 30)) //RDD[List[(String, Double)]]
      .map { list =>
        list.map { case (term, freq) =>
          val wordClassMap: Map[String, String] = Map(
            "n" -> "名詞",
            "nr" -> "人名",
            "nr1" -> "漢語姓氏",
            "nr2" -> "漢語名字",
            "nrj" -> "日語人名",
            "nrf" -> "音譯人名",
            "ns" -> "地名",
            "nsf" -> "音譯地名",
            "nt" -> "機構團體名",
            "nz" -> "其它專名",
            "nl" -> "名詞性慣用語",
            "ng" -> "名詞性語素",
            "nw" -> "新詞",
            "t" -> "時間詞",
            "tg" -> "時間詞性語素",
            "s" -> "處所詞",
            "f" -> "方位詞",
            "v" -> "動詞",
            "vd" -> "副動詞",
            "vn" -> "名動詞",
            "vshi" -> "動詞「是」",
            "vyou" -> "動詞「有」",
            "vf" -> "趨向動詞",
            "vx" -> "形式動詞",
            "vi" -> "不及物動詞",
            "vl" -> "動詞性慣用語",
            "vg" -> "動詞性語素",
            "a" -> "形容詞",
            "ad" -> "副形詞",
            "an" -> "名形詞",
            "ag" -> "形容詞性語素",
            "al" -> "形容詞性慣用語",
            "b" -> "區別詞",
            "bl" -> "區別詞性慣用語",
            "z" -> "狀態詞",
            "r" -> "代詞",
            "rr" -> "人稱代詞",
            "rz" -> "指示代詞",
            "rzt" -> "時間指示代詞",
            "rzs" -> "處所指示代詞",
            "rzv" -> "謂詞性指示代詞",
            "ry" -> "疑問代詞",
            "ryt" -> "時間疑問代詞",
            "rys" -> "處所疑問代詞",
            "ryv" -> "謂詞性疑問代詞",
            "rg" -> "代詞性語素",
            "m" -> "數詞",
            "mq" -> "數量詞",
            "q" -> "量詞",
            "qv" -> "動量詞",
            "qt" -> "時量詞",
            "d" -> "副詞",
            "p" -> "介詞",
            "pba" -> "介詞「把」",
            "pbei" -> "介詞「被」",
            "c" -> "連詞",
            "cc" -> "並列連詞",
            "u" -> "助詞",
            "uzhe" -> "著",
            "ule" -> "了,嘍",
            "uguo" -> "過",
            "ude1" -> "的,底",
            "ude2" -> "地",
            "ude3" -> "得",
            "usuo" -> "所",
            "udeng" -> "等,等等,雲雲",
            "uyy" -> "一樣,一般,似的,般",
            "udh" -> "的話",
            "uls" -> "來講,來說,而言,說來",
            "uzhi" -> "之",
            "ulian" -> "連",
            "e" -> "嘆詞",
            "y" -> "語氣詞",
            "o" -> "擬聲詞",
            "h" -> "前綴",
            "k" -> "後綴",
            "x" -> "字符串",
            "xx" -> "非語素字",
            "xu" -> "網址URL",
            "w" -> "標點符號",
            "wkz" -> "左括號",
            "wky" -> "右括號",
            "wyz" -> "左引號",
            "wyy" -> "右引號",
            "wj" -> "句號",
            "ww" -> "問號",
            "wt" -> "驚嘆號",
            "wd" -> "逗號",
            "wf" -> "分號",
            "wn" -> "頓號",
            "wm" -> "冒號",
            "ws" -> "省略號",
            "wp" -> "破折號",
            "wb" -> "百分號千分號",
            "wh" -> "單位符號")
          val wclass = term.split("/")(1)
          term.replace(wclass, "("+wordClassMap.getOrElse(wclass, "Unknown")+")")
        }
      } //RDD[List[String]]
      .map(_.mkString("\t")).collect()

    proc.foreach(t => println(s"[Res!!]$t"))

    assertResult(1)(1)
  }

}
