package cn.mallcoo.medusa.data.center.web

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import cn.mallcoo.medusa.mql.core.{Parser, Render}
import com.typesafe.config.ConfigFactory
import cn.mallcoo.medusa._
import cn.mallcoo.medusa.data.center.web.ParserActor.{GetSQL, Init}

/**
  * Created by wm on 2017/4/19.
  */

class Listener extends Actor with ActorLogging {

  val parserActor = context.actorOf(Props[ParserActor], "parserActor")

  parserActor ! Init

  override def receive: Receive = LoggingReceive {

    case x: GetSQL => parserActor forward x
    case Init => parserActor forward Init
    case _ =>
  }
}
