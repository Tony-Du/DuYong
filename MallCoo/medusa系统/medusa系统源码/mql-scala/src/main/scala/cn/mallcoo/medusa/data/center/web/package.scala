package cn.mallcoo.medusa.data.center

import spray.json.DefaultJsonProtocol._

/**
  * Created by whimmy on 17/06/10.
  */
package object web {

  case class Respense(code: Int, msg: String, results: List[String])


  implicit val respenseFormat = jsonFormat3(Respense)
}
