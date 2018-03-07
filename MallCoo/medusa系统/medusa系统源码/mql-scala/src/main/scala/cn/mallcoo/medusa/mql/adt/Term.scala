package cn.mallcoo.medusa.mql.adt

import cn.mallcoo.medusa.mql.adt.Term._
import cn.mallcoo.medusa.mql.core.RenderException
import net.minidev.json.{JSONArray, JSONObject, JSONValue}
import cn.mallcoo.medusa.mql._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by wm on 2017/5/23.
  */

trait Term {
  def map(f: Term => Term): Term

  def apply(key: String): Term = EmptyTerm

  def join(sep: String): Term = join("", sep, "")

  def join(start: String, sep: String, end: String): Term

  def replace(regx: String, replacement: String): Term

  def group(string: String): Term

  def as[A <: Term]: A = this.asInstanceOf[A]

  def has(term: Term): Boolean

  def has(term: String): Boolean = has(StringTerm(term))

  def union(term: Term): Term

  def +(term: Term): Term

  def distinct: Term

  def flatten: Term

  def filter(f: Term => Boolean): Term

  def getOrElse(key: String, default: => Term): Term = {
    apply(key) match {
      case EmptyTerm => default
      //      case StringTerm(x) if x.trim.isEmpty => default
      case x => x
    }
  }

  def isEmpty: Boolean

  def zipWithIndex(init: Int): Term

  def size: Int

  def add(key:String,term: Term): Term
}

trait ValueTerm[V] extends Term {
  val value: V

  override def join(start: String, sep: String, end: String): Term = StringTerm(start + toString + end)

  override def map(f: (Term) => Term): Term = f(this)

  override def group(string: String): Term = this

  override def toString: String = value.toString

  override def has(term: Term): Boolean = false

  override def union(term: Term): Term = term match {
    case EmptyTerm => this
    case _ => ListTerm(this :: term :: Nil)
  }

  override def +(term: Term): Term = term match {
    case EmptyTerm => this
    case _ => ListTerm(this :: term :: Nil)
  }

  override def distinct: Term = this

  override def flatten: Term = this

  override def filter(f: (Term) => Boolean): Term = if (f(this)) this else EmptyTerm

  override def isEmpty: Boolean = value == null

  override def zipWithIndex(init: Int): Term = ListTerm(ObjectTerm(Map(
    "index" -> NumberTerm(init),
    "value" -> this
  )) :: Nil)

  override def size: Int = 1

  override def replace(regx: String, replacement: String): Term = this

  override def add(key: String, term: Term): Term = this
}

object Term {
  def parse(o: java.lang.Object): Term = o match {
    case o: JSONObject =>
      ObjectTerm(o.asScala.map(x => x._1.toLowerCase -> parse(x._2)).toMap)
    case a: JSONArray =>
      if (a.size() == 0) EmptyTerm
      else ListTerm(a.asScala.toList.map(x => parse(x)))
    case s: String => StringTerm(s)
    case n: Number => NumberTerm(n)
    case b: java.lang.Boolean => BooleanTerm(b)
    case null => EmptyTerm
  }

  def apply(string: String): Term = parse(JSONValue.parse(string.toString))

  def apply(key: String, value: Term): Term = ObjectTerm(Map(key -> value))

  def apply(map: Map[String, Term]): Term = ObjectTerm(map)


  @tailrec
  def getTerm(list: List[String], term: Term): Term =
    if (term == EmptyTerm) term
    else list match {
      case Nil => term
      case head :: tail => getTerm(tail, term(head))
    }

  def getTerm(path: String, term: Term): Term = getTerm(path.split("\\.").toList, term)

  case class StringTerm(value: String) extends ValueTerm[String] {
    override def isEmpty = value.trim.isEmpty

    def ltrim = value match {
      case leftEmptyString(x) => StringTerm(x + " ")
      case _ => this
    }


    def +(string: String) = StringTerm(value + string)

    //    override def toString: String = "\"" + value + "\""
    override def replace(regx: String, replacement: String): Term = {
      val list = replacementRegex.findAllIn(replacement).toList

      StringTerm(regx.r.replaceAllIn(value, x => {
        list.map{
          case num if num startsWith "#" => x.group(num.substring(1).toInt)
          case y => y
        }.mkString("")
      }))
    }
  }

  case class NumberTerm(value: Number) extends ValueTerm[Number]

  case class BooleanTerm(value: Boolean) extends ValueTerm[Boolean]

  case class ListTerm(value: List[Term]) extends Term {
    override def toString: String = value.mkString("[ ", ", ", " ]")

    override def map(f: (Term) => Term): Term = ListTerm(value.map(f))

    override def join(start: String, sep: String, end: String): Term = {
      val noneEmpty = value.filterNot(x => x == EmptyTerm || x.toString.trim.isEmpty)
      StringTerm(
        if (noneEmpty.isEmpty) ""
        else noneEmpty.map {
          case StringTerm(x) => x
          case NumberTerm(n) => n
          case BooleanTerm(b) => if (b) "true" else "false"
        }.mkString(start, sep, end))
    }

    override def group(string: String): Term = {
      val list = value.map {
        case ObjectTerm(map) => map.map(x => x._1 -> x._2)
      }.groupBy(x => x(string)).values.map { m =>
        val map = m.reduce { (x, y) =>
          x.map { p =>
            val array = ((p._2, y.get(p._1)) match {
              case (ListTerm(i), Some(ListTerm(j))) => i ::: j
              case (ListTerm(i), Some(j)) => j :: i
              case (i, Some(ListTerm(j))) => i :: j
              case (i, Some(j)) => i :: j :: Nil
              case (i, None) => i :: Nil
            }).distinct

            p._1 -> (if (array.size == 1) array.head else ListTerm(array))

          }
        }

        ObjectTerm(map)
      }.toList

      ListTerm(list)
    }

    override def has(term: Term): Boolean = value.contains(term)

    override def union(term: Term): Term = term match {
      case EmptyTerm => this
      case ListTerm(x) => ListTerm(value ::: x)
      case _ => ListTerm(term :: value)
    }

    override def +(term: Term): Term = term match {
      case EmptyTerm => this
      case ListTerm(x) => ListTerm(value ::: x)
      case _ => ListTerm(term :: value)
    }

    override def distinct: Term = ListTerm(value.distinct)

    override def flatten: Term = ListTerm(flatten(value, Nil))

    private def flatten(in: List[Term], out: List[Term]): List[Term] = in match {
      case Nil => out
      case ListTerm(xs) :: tail => flatten(xs ::: tail, out)
      case x :: tail => flatten(tail, x :: out)
    }

    override def filter(f: Term => Boolean): Term = {
      val r = value.filter(f)
      if (r.isEmpty) EmptyTerm else ListTerm(r)
    }

    override def apply(key: String): Term = try {
      value(key.toInt)
    } catch {
      case _: Exception => EmptyTerm
    }

    override def isEmpty: Boolean = value.isEmpty

    override def zipWithIndex(init: Int): Term = ListTerm(value.zipWithIndex.map { x =>
      ObjectTerm(Map(
        "index" -> NumberTerm(x._2 + init),
        "value" -> x._1
      ))
    })

    override def size: Int = value.size

    def removeObject(item: (String, ValueTerm[_])) = ListTerm(value.filterNot {
      case x: ObjectTerm => x.value.contains(item._1) && x(item._1).toString == item._2.toString
      case _ => false
    })

    override def replace(regx: String, replacement: String): Term = this

    override def add(key: String, term: Term): Term = this
  }

  case class ObjectTerm(value: Map[String, Term]) extends Term {
    override def toString: String = value.map(x => s""" "${x._1}":${x._2} """).mkString("{", ",", "}")

    override def map(f: (Term) => Term): Term = f(this)

    override def apply(key: String): Term = if (key == "head") value.head._2
    else value.get(key) match {
      case Some(t) => t match {
        case ObjectTerm(x) if x.isEmpty => EmptyTerm
        case o: ObjectTerm => o
        case ListTerm(Nil) => EmptyTerm
        case l: ListTerm => l
        case v: ValueTerm[_] => v
        case _ => EmptyTerm
      }
      case None => EmptyTerm
    }

    override def join(start: String, sep: String, end: String): Term = throw RenderException(s"$toString can't join $sep")

    override def group(string: String): Term = this

    override def has(term: Term): Boolean = term match {
      case StringTerm(s) => value.contains(s)
      case _ => false
    }

    override def union(term: Term): Term = term match {
      case EmptyTerm => this
      //      case ObjetTerm(x) => ObjetTerm(value ++ x)
      case _ => ListTerm(this :: term :: Nil)
    }

    override def +(term: Term): Term = term match {
      case EmptyTerm => this
      case ObjectTerm(x) => ObjectTerm(value ++ x)
      case x => throw RenderException(s"$toString can't add $x")
    }

    override def distinct: Term = this

    override def flatten: Term = this

    override def filter(f: (Term) => Boolean): Term = if (f(this)) this else EmptyTerm

    override def isEmpty: Boolean = value.isEmpty

    override def zipWithIndex(init: Int): Term = ListTerm(ObjectTerm(Map(
      "index" -> NumberTerm(init),
      "value" -> this
    )) :: Nil)

    override def size: Int = 1

    def update(path: String, newValue: Term): ObjectTerm = {

      def assemble(list: List[(String, Term)], result: ObjectTerm): ObjectTerm = list match {
        case Nil => result("root").as[ObjectTerm]
        case (k, v) :: tail =>
          assemble(tail, ObjectTerm(Map(k -> (v + result))))
      }

      def modify(keys: List[String], term: Term, result: List[(String, ObjectTerm)], name: String): ObjectTerm = (keys, term) match {
        case (Nil, _) => assemble(result, ObjectTerm(Map(name -> newValue)))
        case (key :: tail, t: ObjectTerm) =>
          modify(tail, term(key), (name -> ObjectTerm(term.as[ObjectTerm].value.filterNot(_._1 == key))) :: result, key)
        case (_, _) => modify(Nil, term, result, name)
      }

      val values = path.split("\\.")
      modify(values.toList, this, Nil, "root")
    }

    override def replace(regx: String, replacement: String): Term = this

    override def add(key: String, term: Term): Term = ObjectTerm(value + (key -> term))

  }

  case object EmptyTerm extends Term {
    override def map(f: (Term) => Term): Term = this

    override def group(string: String): Term = this

    override def join(start: String, sep: String, end: String): Term = this

    override def has(term: Term): Boolean = false

    override def union(term: Term): Term = term

    override def +(term: Term): Term = term

    override def distinct: Term = this

    override def flatten: Term = this

    override def toString: String = ""

    override def filter(f: (Term) => Boolean): Term = this

    override def isEmpty: Boolean = true

    override def zipWithIndex(init: Int): Term = this

    override def size: Int = 0

    override def replace(regx: String, replacement: String): Term = this

    override def add(key: String, term: Term): Term = this
  }

  case class RenderTerm[+A <: Term](root: Term, parent: Term, current: A) extends Term {
    override def apply(key: String): RenderTerm[Term] = this.copy(current = current.getOrElse(key, parent.getOrElse(key, root(key))))

    //    override def as[B <: Term]: RenderTerm[B] = this.copy(current = current.asInstanceOf[B])

    override def map(f: (Term) => Term): RenderTerm[Term] = this.copy(current = current.map(f))

    override def join(start: String, sep: String, end: String): RenderTerm[Term] = this.copy(current = current.join(start, sep, end))

    override def add(key: String, term: Term): RenderTerm[Term] = this.copy(current = current.add(key, term))

    override def replace(regx: String, replacement: String): RenderTerm[Term] = this.copy(current = current.replace(regx, replacement))

    override def group(string: String): RenderTerm[Term] = this.copy(current = current.group(string))

    override def has(term: Term): Boolean = current.has(term)

    override def union(term: Term): RenderTerm[Term] = term match {
      case RenderTerm(_, _, x) => this.copy(current = current.union(x))
      case _ => this.copy(current = current.union(term))
    }

    override def +(term: Term): RenderTerm[Term] = term match {
      case RenderTerm(_, _, x) => this.copy(current = current + x)
      case _ => this.copy(current = current + term)
    }

    override def distinct: RenderTerm[Term] = this.copy(current = current.distinct)

    override def flatten: RenderTerm[Term] = this.copy(current = current.flatten)

    override def filter(f: Term => Boolean): RenderTerm[Term] = this.copy(current = current.filter(f))

    override def isEmpty: Boolean = current.isEmpty

    override def zipWithIndex(init: Int): RenderTerm[Term] = this.copy(current = current.zipWithIndex(init))

    override def size: Int = current.size

    override def toString: String = current.toString
  }

  object RenderTerm {
    def apply(current: Term): RenderTerm[Term] = RenderTerm(current, current, current)
  }

}








