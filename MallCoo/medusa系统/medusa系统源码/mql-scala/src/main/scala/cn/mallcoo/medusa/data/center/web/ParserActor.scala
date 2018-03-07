package cn.mallcoo.medusa.data.center.web

import akka.actor.{Actor, ActorLogging}
import cn.mallcoo.medusa.data.center.web.ParserActor.{GetSQL, Init}
import cn.mallcoo.medusa.mql.MQLParser
import cn.mallcoo.medusa.mql.adt.Term
import cn.mallcoo.medusa.mql.adt.Term.{EmptyTerm, ListTerm, ObjectTerm, StringTerm}

/**
  * Created by whimmy on 17/06/10.
  */

object ParserActor {

  case class GetSQL(parameter: String)

  case object Init

}

class ParserActor extends Actor with ActorLogging {

  var parser: MQLParser = _

  override def receive: Receive = {
    case Init =>
      val result = try {
        val p = new MQLParser
        if (p.test) {
          parser = p
          Respense(200, "load config success", Nil)
        }
        else Respense(4001, "load config faild", Nil)
      } catch {
        case ex: Exception =>
          log.error(ex,"load config faild")
          Respense(401, "load config faild", Nil)
      }
      sender() ! result
    case GetSQL(parameter) =>
      val result = try {

        val mqls = parser.renders(parameter)
        //        val mql = parser.render(term("model").toString, term("parameter"))
        log.info(s"\nparameter:\n$parameter\nmql:${mqls.mkString("\n","\n","\n")}")
        Respense(200, "", mqls)
      } catch {
        case x: NoSuchElementException =>
          log.error(x,"no such model")
          Respense(4002, "no such model", Nil)
        case y: Exception =>
          log.error(y,"cannot render")
          Respense(4003, "cannot render", Nil)
      }
      sender() ! result

    case _ =>
  }
}
