import cn.mallcoo.medusa.mql._
import cn.mallcoo.medusa.mql.adt.Term.StringTerm

/**
  * Created by wm on 2017-07 23:54.
  */
object abc {
  def main(args: Array[String]): Unit = {

    val tag =
      """
        |17年2-4卡券核销用户,
        |16年8-10电影票购买用户,
        |16年车场缴费用户,
        |16年团购核销用户,
        |上班族用户,
        |停车券投放前100高价值客户,
        |线上活跃用户,
        |异常积分预警人群,
        |电影券投放高价值客户,
        |停车券拉回高价值客户,
        |团购投放高价值客户,
        |团购拉回流失高价值客户,
        |电影券拉回流失高价值客户,
        |优惠券/代金券拉回流失高价值客户,
        |优惠券/代金券投放高价值客户,
        |线上月活会员数,
        |活跃会员数,
        |家庭客群,
        |商场工作人员,
        |周末到场购买儿童业态用户,
        |工作日用户,
        |停车券投放高价值客户,
        |text,
        |蛋黄哥核销用户617-703
      """.stripMargin.split(",").toList.map(x => (x.trim, x.trim.split("|").toList))

    val tags = tag.map(x => (x._1,x._1::Nil)) ::: tag


    val input = Protocol.readTextFile("D:\\svn\\Hadoop\\codes\\zhangweiming\\mql-scala\\src\\test\\resources\\bac")
    val lines = input.split("\n")
    val r = lines.map { line =>
      val row = line.split("\t")
      (row(0).split("\\|"), row(0))
    }.toList.map { x =>
      (tags.filter(t => t._2.forall(x._1.contains(_))).map(_._1),x._2)
    }.filterNot(_._1.isEmpty).map { x =>
      s"""insert into table medusa.tag2 select mallcoo_id,'|${x._1.mkString("|")}|',projectid from medusa.tag where tag='${x._2}';"""
    }
//    r.foreach(println)



    val list = replacementRegex.findAllIn("udf.to_num(#1)").toList

    val regx = "(\\w+/\\w+)"


    val value = "_rent_max__day/_shop_area_max__day*12/365"
    val abc = regx.r.replaceAllIn(value, x => {

      println(x.group(1))

      x.group(1)


      list.map{
        case num if num startsWith "#" =>
//          println(x.group(num.substring(1).toInt))
          println(num.substring(1).toInt)
          x.group(num.substring(1).toInt)
        case y =>
          y
      }.mkString("")
    })

//    list.foreach(println)
    println(abc)


//    println(tags.mkString("\n"))
  }
}
