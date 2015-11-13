package com.kmlab.main

import com.kmlab.main
import utils.SparkFunSuite

/**
 * Created by WeiChen on 2015/10/7.
 */
class TextSegmentTest extends SparkFunSuite {
  localTest("ANSJ NLP Test") { sc =>
    val text = Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減")
    val input = sc.parallelize(text)
    val result = ANSJ.ansj(input)
    result.collect().foreach(t => println("[NLP]" + t))
    assertResult(1)(1)
  }

  localTest("ANSJ To Test") { sc =>
    val text = Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減")
    val input = sc.parallelize(text)
    val result = main.ANSJ.ansj(input, "To")
    result.collect().foreach(t => println("[NLP]" + t))
    assertResult(1)(1)
  }

  localTest("ZHConverter Test") { sc =>
    val text = Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減",
      "昨天營收結果略高於預估值，好消息公告後激勵聯發科壓抑已久的股價走勢，昨日股價帶量上攻，終場大漲5.31％、以258元作收，向上挑戰月線、季線。 聯發科昨天並公告完成對類比IC廠立錡的股權完成51％收購作業。聯發科表示，透過旭思投資在昨日下午3點30分完成公開收購立錡普通股75,743,826股（為立錡應賣股份的66.56％），由於按收購時程，得在今年底前完成51％，明年第2季完成100％，因此還是會依比例退還股東多收的股權。依聯發科合併立錡的時程來看，預計該次收購作業會在今年第4季按權益法認列立錡51％獲利，到明年第2季則可計入合併營收。 目前聯發科仍以大陸及新興國家市場為主，受到市場變化快速，第3季傳統旺季接單、出貨變化也快速。今年7月份下修手機晶片出貨預估時，市場對聯發科下半年多持悲觀看法，不過，新台幣走貶、客戶端庫存去化、以及智慧型手機晶片銷售優於預期，反而讓聯發科交出超標的好成績。 聯發科走出客戶庫存調整壓力區，加上手機晶片銷售優於預期，9月合併營收跨過200億元，達200.41億元、創下單月營收歷史第3高，較8月成長5.4％；累計第3季合併營收569.62億元，季增21.08％，優於7月底法說預估的季成長10％∼18％水準。進入第4季，聯發科仍維持傳統淡季看法。 聯發科副董事長謝清江在今年第3季於公開場合出現時，始終維持一致的預估值看法認為，晶片價格競爭更為激烈，預估第3季旺季呈現「溫和」成長走勢，預估第3季營收約517億∼555億元，季成長10％∼18％。")
    val rdd = sc.parallelize(text)
    println("[InputLength]" + rdd.count())
    val cn = main.ANSJ.toSimplified(rdd)
    main.ANSJ.ansj(cn)
    rdd.collect().foreach(t => println("[ZH_CN]" + t))
    assertResult(1)(1)
  }

  localTest("Top N Test") { sc =>
    val text = sc.parallelize(Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減"))
    val result = main.ANSJ.ansjTopN(text, 10)
    result.collect().foreach(t => println("[Top N]" + t))
    assertResult(1)(1)
  }

  localTest("ANSJ Top N Test") { sc =>
    val text = sc.parallelize(Array("昨天台股順利突破季線，今天趁勝追擊續攻前波高點8488點，早盤開平高站上8400點後，隨著DRAM股華亞科、南科大漲，台塑四寶也回穩，台積電也維持1.5%左右的漲幅，推升指數持續震盪向上，中場在8450點附近盤整，尾盤拉高順利衝過8488點，終場大漲101.13點，以8495.23點作收，成交量928.86億元。 櫃檯指數今天表現也不弱，在世界等半導體股跟進集中市場表現帶動下，指數也呈現開平走高，終場上漲1.48點，收在最高點122.25點，成交量251.04億元。 台股今天以生醫類股表現最佳，漲幅逾3.5%，橡膠、塑膠、化學生醫、造紙、水泥漲幅也逾2%；半導體、油電燃氣、汽車、玻陶等也有1.5%以上的漲幅。 南科總經理高啟全離職的衝擊已經大幅減"))
    val result = main.ANSJ.ansj(text, 10)
    result.collect().foreach(t => println("[ANSJ Top N]" + t))
    assertResult(1)(1)
  }
}