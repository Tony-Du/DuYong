import cn.mallcoo.medusa._
import cn.mallcoo.medusa.mql.test.core.Term.RenderTerm
import cn.mallcoo.medusa.mql.test.core._
import com.typesafe.config.ConfigFactory

/**
  * Created by wm on 2017/5/22.
  */
object ParserTest2 {
  def main(args: Array[String]): Unit = {

    //    val parser = new MQLParser().setCompress(true)
    //    println(parser.test)


    val parser = Parser(ConfigFactory.load("abc").as[Map[String, String]]("settings"))
//    val render = Render(ConfigFactory.load("symbol").as[Map[String, String]]("symbol"), parser)
//    val config = RenderConfig(true)
//
//    val models: Map[String, Rule] = ConfigFactory.load("mql").as[Map[String, String]]("mql").map { x =>
//      x._1 -> render.render(render.parser.parse(x._2), RenderContext(render.rule)) _
//    }
//
//    def renders(parameter: String): List[String] = {
//      val term = Term(parameter).as[ObjectTerm].value
//      val para = term("parameter").as[ObjectTerm]
//      val paras: List[Term] = if (Term.getTerm("time_range", para).size > 1) {
//        para("time_range").as[ListTerm].value.map { t =>
//          val p = para + ObjectTerm(Map("time_range" -> ListTerm(t :: Nil)))
//          if (t.as[ListTerm].value.contains(EmptyTerm)) {
//            Term.getTerm("dimensions.event", para) match {
//              case EmptyTerm => p
//              case l: ListTerm => p.as[ObjectTerm].update("dimensions.event", l.removeObject("field", StringTerm("time")))
//              case _ => p
//            }
//          } else p
//        }
//      } else if (term("model").toString == "user_list" && para("model").toString != "export") {
//        para :: para + ObjectTerm(Map("model" -> StringTerm("export"), "pages" -> ObjectTerm(Map("size" -> NumberTerm(1000))))) :: Nil
//      } else if (term("model").toString == "tag_mark" && para("type").toString != "count" && para("type").toString != "remove") {
//        para :: para + ObjectTerm(Map("type" -> StringTerm("count"))) :: Nil
//      }
//      else para :: Nil
//      paras.map(x => models(term("model").toString)(config)(RenderTerm(x)).toString)
//      //    paras.map(x => render(term("model").toString, x, config))
//    }


//    def test = {
//      val test = ConfigFactory.load("test").as[Map[String, Map[String, Map[String, String]]]]("test")
//
//      val r = test.flatMap { x =>
//        x._2.map { y =>
//          val name = y._1
//          val t = y._2
//          val result = renders(t("parameter")).mkString("\n")
//
//          val model = s"${x._1},$name"
//          println(s"$model\n$result\n${t("mql")}")
//
//          model -> (result == t("mql").split("\n").filterNot(_.trim.isEmpty).mkString("\n"))
//        }
//      }.filterNot(x => x._2)
//      println(r)
//      r.isEmpty
//    }

//        val parameter = ConfigFactory.load("test").as[String]("test.segmentation.自定义指标无分组用户过滤加单事件过滤.parameter")
//    val mql = ConfigFactory.load("mql").as[String]("mql.segmentation")

    val mql =
      """
        |{{
        |  #root#.union(#root.dimensions.user#,#root.dimensions.event#).map {
        |        e -> #event_type#,
        |        a -> #alias#
        |      => match {
        |          #e# is 'empty' => #a#,
        |          _ => ''
        |      }
        |  }.join(',').prefix('PARTITION BY ')
        |}}
        |FROM abc 'nvf' acd;
      """.stripMargin
    val tree = parser.parse(mql)
    println(tree.mkString("  "))

    val parameter =
      """
            {"parameter":{"filter":{"event":{"query":{"must":[{"code":"event_type","operator":"in","values":[25001]},{"values":[],"query":{"should":[{"values":[],"query":{"must":[{"code":"event_type","operator":"equals","values":[25001]}]}}]}}]}}},"measures":[{"event_type":25001,"measure":"distinct_totalcount","alias":"distinct_totalcount_25001","group":true}],"expressions":[{"expression":"distinct_totalcount_25001","alias":"distinct_totalcount_25001"}],"dimensions":{"user":[{"field":"_register_source","alias":"_register_source_dim"}],"event":[{"field":"time","alias":"day_dim","preset":"day"}]},"time_range":[["2016-06-20","2016-06-30"],[null,"2016-06-20"]],"projectid":1,"category":"user","is_extend":false,"is_any":false},"model":"segmentation"}
      """.stripMargin

    val render = Render(ConfigFactory.load("symbol").as[Map[String, String]]("symbol"), parser)

    val r = render.render(tree,RenderContext.empty)(RenderConfig(false))(RenderTerm(Term(parameter)))

    println(r)
  }
}
