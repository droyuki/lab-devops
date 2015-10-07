package bigboost.main

import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/10/7.
 */
class TextSegmentTest extends SparkFunSuite {
  localTest("ANSJ Test") { sc =>
    val text = Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減")
    val input = sc.parallelize(text)
    val result = bigboost.main.TextSegmentation.ansj(input)
    println("[Start]" + result.count())
    val res = result.collect()
    res.foreach(t => println(t + ", "))
    assertResult(1)(1)
  }

}
