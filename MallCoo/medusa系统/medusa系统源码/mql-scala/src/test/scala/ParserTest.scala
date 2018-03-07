import cn.mallcoo.medusa.mql.MQLParser

/**
  * Created by wm on 2017/5/22.
  */
object ParserTest {
  def main(args: Array[String]): Unit = {

        val parser = new MQLParser().setCompress(true)
        println(parser.test)
  }
}
