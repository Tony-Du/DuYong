package cn.mallcoo.medusa.mql

import cn.mallcoo.medusa.mql.core.{Parser, Render, RenderConfig, RenderContext}
import com.typesafe.config.ConfigFactory
import cn.mallcoo.medusa._
import cn.mallcoo.medusa.mql.adt.Term
import cn.mallcoo.medusa.mql.adt.Term._
import cn.mallcoo.medusa.mql.extend.ExpressionParser
import cn.mallcoo.medusa.mql.extend.ExpressionParser.DecomposeTree


/**
  * Created by whimmy on 17/06/10.
  */
class MQLParser {

  var config = RenderConfig(true)

  def setCompress(compress: Boolean) = {
    this.config = RenderConfig(compress)
    this
  }

  val parser = Parser(ConfigFactory.load("settings").as[Map[String, String]]("settings"))
  val render = Render(ConfigFactory.load("symbol").as[Map[String, String]]("symbol"), parser)

  val models: Map[String, Rule] = ConfigFactory.load("mql").as[Map[String, String]]("mql").map { x =>
    x._1 -> render.render(render.parser.parse(x._2), RenderContext(render.rule)) _
  }

  def render(rule: Rule)(parameter: String, config: RenderConfig = RenderConfig(true)): List[String] = {
    val term = Term(parameter).as[ObjectTerm].value
    val para = term("parameter").as[ObjectTerm]
    val paras: List[Term] = if (Term.getTerm("time_range", para).size > 1) {
      para("time_range").as[ListTerm].value.map(t => para + ObjectTerm(Map("time_range" -> ListTerm(t :: Nil))))
    } else if (term("model").toString == "user_list" && para("model").toString != "export" && para("model").toString != "count") {
      para :: para + ObjectTerm(Map("model" -> StringTerm("export"), "pages" -> ObjectTerm(Map("size" -> NumberTerm(1000))))) :: Nil
    } else if (term("model").toString == "tag_mark" && para("type").toString != "count" && para("type").toString != "remove") {
      para :: para + ObjectTerm(Map("type" -> StringTerm("count"))) :: Nil
    }
    else para :: Nil

    val parameters = paras.map { p =>
      if (p.as[ObjectTerm].value.contains("time_range") && p("time_range").as[ListTerm].value.head.as[ListTerm].value.contains(EmptyTerm)) {
        Term.getTerm("dimensions.event", p) match {
          case EmptyTerm => p
          case l: ListTerm => p.as[ObjectTerm].update("dimensions.event", l.removeObject("field", StringTerm("time")))
          case _ => p
        }
      } else p
    }

    val r = parameters.map { x =>
      val out = expressions(x)

      val top = out("top_event") match {
        case o: ObjectTerm => ObjectTerm(out.value + ("top_event" -> expressions(o)))
        case _ => out
      }

      top("event_filters") match {
        case ListTerm(values) => ObjectTerm(top.value + ("event_filters" -> ListTerm(values.map(expressions))))
        case _ => top
      }
    }

    r.map(x => rule(config)(RenderTerm(x)).toString)
  }

  def expressions(term: Term): ObjectTerm = term("expressions") match {
    case ListTerm(value) =>
      val exper = value.zipWithIndex.map { z =>
        val expression = z._1.as[ObjectTerm].value
        val tree = ExpressionParser.render(z._1("expression").as[StringTerm].value)
        val decompose = ExpressionParser.decompose(tree, DecomposeTree(z._2, 0, "", Nil))
        val expression2 =
          expression +
            ("expression" -> StringTerm(decompose.root)) +
            ("children" -> ListTerm(decompose.parts.map(StringTerm))) +
            ("raw" -> StringTerm(tree.toString))
        expression.get("limits") match {
          case Some(ListTerm(ObjectTerm(limit) :: Nil)) =>
            ObjectTerm(expression2 + ("limits" -> ListTerm(ObjectTerm(limit + ("expression" -> StringTerm(tree.toStringWithoutAgg))) :: Nil)))
          case _ => ObjectTerm(expression2)
        }
      }
      ObjectTerm(term.as[ObjectTerm].value + ("expressions2" -> ListTerm(exper)))

    case _ => term.as[ObjectTerm]
  }


  def renders(parameter: String): List[String] = {
    val term = Term(parameter).as[ObjectTerm].value
    render(models(term("model").toString))(parameter)
  }


  def test = {
    val test = ConfigFactory.load("test").as[Map[String, Map[String, Map[String, String]]]]("test")

    val r = test.flatMap { x =>
      x._2.map { y =>
        val name = y._1
        val t = y._2
        val model = s"${x._1},$name"
        println(model)
        val result = renders(t("parameter")).mkString("\n")
        //        val term = Term(t("parameter")).as[ObjectTerm].value
        //        val result = render(term("model").toString, term("parameter"))


        println(s"$result\n${t("mql")}")

        model -> (result == t("mql").split("\n").filterNot(_.trim.isEmpty).mkString("\n"))
      }
    }.filterNot(x => x._2)
    //    r.forall(x => x)
    println(r)
    r.isEmpty
  }
}
