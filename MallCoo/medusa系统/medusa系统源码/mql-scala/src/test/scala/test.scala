import cn.mallcoo.medusa.mql.adt.Term._
import cn.mallcoo.medusa.mql._
import cn.mallcoo.medusa.mql.adt.Term
import cn.mallcoo.medusa.mql.core.{Parser, Render, RenderConfig, RenderContext}
import com.typesafe.config.ConfigFactory
import cn.mallcoo.medusa._

import scala.annotation.tailrec
import scala.util.matching.Regex

/**
  * Created by whimmy on 17/06/03.
  */
object test {
  def main(args: Array[String]): Unit = {

    //        val parameter =
    //          """
    //            {"parameter":{"filter":{"event":{"query":{"must":[{"code":"event_type","operator":"in","values":[1001,1002]},{"values":[],"query":{"should":[{"values":[],"query":{"must":[{"code":"event_type","operator":"equals","values":[1001]}]}},{"values":[],"query":{"must":[{"code":"event_type","operator":"equals","values":[1002]}]}}]}}]}}},"measures_groups":[{"period":"month","measures":[{"measure":"distinct_sum","field":"_sales_point","alias":"_sales_point_distinct_sum__month","group":true},{"event_type":1001,"measure":"sum","field":"total_amount","alias":"total_amount_sum_1001_month","group":true},{"measure":"distinct_sum","field":"_rent","alias":"_rent_distinct_sum__month","group":true},{"event_type":1002,"measure":"totalcount","alias":"totalcount_1002_month","group":true},{"measure":"distinct_sum","field":"_shop_area","alias":"_shop_area_distinct_sum__month","group":true}]}],"expressions":[{"expression":"sum(greatest(_sales_point_distinct_sum__month/100*total_amount_sum_1001_month-_rent_distinct_sum__month,0))","alias":"customMeasure235_1001","period":"month","format":"double"},{"expression":"avg(totalcount_1002_month/_shop_area_distinct_sum__month)","alias":"customMeasure208_1002","period":"month","format":"double"}],"dimensions":{"user":[{"field":"_floor_name","alias":"_floor_name_dim"}],"event":[]},"time_range":[["2017-04-01","2017-06-30"]],"projectid":3,"category":"shop","is_extend":false,"is_any":false},"model":"segmentation"}
    //          """.stripMargin

    val parameter =
      """
        |{"parameter":{"filter":{},"measures":[{"measure":"distinct_sum","field":"_rent","alias":"_rent_distinct_sum__month","group":true}],"properties":[],"dimensions":{"user":[{"field":"_mall_name","alias":"_mall_name_dim"}],"event":[]},"projectid":2,"category":"shop","is_extend":false,"is_any":false,"expressions":[{"expression":"sum(_rent_distinct_sum__month)","alias":"index_count"}]},"model":"user_analytics"}
      """.stripMargin


    //    val parameter = ConfigFactory.load("test").as[String]("test.segmentation.自定义指标无分组用户过滤加单事件过滤.parameter")

    //    val parser = cn.mallcoo.medusa.mql.test.core.Parser(ConfigFactory.load("settings").as[Map[String, String]]("settings"))
    //    parser.parse(ConfigFactory.load("mql").as[String]("mql.segmentation"))


//    val result = new cn.mallcoo.medusa.mql.MQLParser().setCompress(true).renders(parameter)
//    println(result.mkString("\n").replaceAll("""\n+\s*\n""", "\n"))


    //    val p = Term(parameter)


        val mql = ConfigFactory.load("mql").as[String]("mql.user_analytics")

    val parser = Parser(ConfigFactory.load("settings").as[Map[String, String]]("settings"))
    val render = Render(ConfigFactory.load("symbol").as[Map[String, String]]("symbol"), parser)
    val renderConfig = RenderConfig(true)
    val rule = render.render(render.parser.parse(mql), RenderContext(render.rule)) _
    val results = new cn.mallcoo.medusa.mql.MQLParser().render(rule)(parameter, renderConfig)
    results.foreach(x => println(x.replaceAll("""\n+\s*\n""", "\n")))


  }
}
