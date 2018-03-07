package cn.mallcoo.medusa.mql.test.core

/**
  * Created by wm on 2017/6/27.
  */
case class RenderContext(settings: Map[String, Rule])

object RenderContext {
  val empty = RenderContext(Map.empty)
}