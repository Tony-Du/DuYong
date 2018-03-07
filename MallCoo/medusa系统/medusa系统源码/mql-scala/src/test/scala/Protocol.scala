import scala.io.Source

/**
  * Created by wm on 2017/4/14.
  */
object Protocol {

  case class Context(settings: String, compress: Boolean = false)
  case class Statements(variables:List[String],mqls:List[String])


  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      if (resource != null)
        resource.close()
    }


  def readTextFile(filename: String): String = {
    try {
      using(io.Source.fromFile(filename))(_.mkString)
    } catch {
      case e: Exception => ""
    }
  }

  def readResourcesFile(filename: String): String = {
    try {
      using(Source.fromURL(getClass.getResource(filename)))(_.mkString)
    } catch {
      case e: Exception => ""
    }
  }
}
