package cn.mallcoo.medusa.mql.core

import cn.mallcoo.medusa.mql._
import cn.mallcoo.medusa.mql.adt.AST._
import cn.mallcoo.medusa.mql.adt.Term._
import cn.mallcoo.medusa.mql.adt._

import scala.annotation.tailrec

/**
  * Created by whimmy on 17/06/02.
  */

case class RenderException(message: String) extends RuntimeException(message)

case class RenderConfig(compress: Boolean)

case class Render(symbol: Map[String, String], parser: Parser) {

  val settings = parser.globalSetting

  @tailrec
  private def settingsRender(keys: List[String], rule: Map[String, Rule]): Map[String, Rule] = keys match {
    case Nil => rule
    case head :: tail =>
      val r = renderSetting(head, findAllDependencies(List(head :: Nil)), rule)
      settingsRender(tail, r)
  }

  @tailrec
  private def renderSetting(key: String, references: List[String], rule: Map[String, Rule]): Map[String, Rule] = references match {
    case Nil => rule + (key -> render(settings(key)._1, RenderContext(rule)) _)
    case head :: tail =>
      val r = rule + (head -> render(settings(head)._1, RenderContext(rule)) _)
      renderSetting(key, tail, r)
  }

  @tailrec
  private def findAllDependencies(dependencies: List[List[String]]): List[String] =
    if (dependencies.forall(_.head == "NOMORE")) dependencies.flatMap(_.tail.init).distinct
    else {
      val deps = dependencies.flatMap { x =>
        x.head match {
          case "NOMORE" => x :: Nil
          case key =>
            val references = settings(key)._2.distinct
            val ref = if (references.isEmpty) List("NOMORE" :: x) else references.map(_ :: x)
            if (ref.exists(x => x.size != x.distinct.size)) throw ParseException("global settings exist circular reference")
            ref
        }

      }
      findAllDependencies(deps)
    }


  val rule = settingsRender(settings.keys.toList, Map.empty)

  def render(asts: List[AST], context: RenderContext)(config: RenderConfig)(root: RenderTerm[Term]): Term = {

    def trim(sql: StringTerm, string: String): StringTerm =
      if (!config.compress) sql + string
      else string match {
        case x@connectStringRegex(_*) =>
          val s = sql.value
          StringTerm(
            if (s.endsWith(" ")) s.substring(0, s.length - 1) + x
            else if (s.endsWith(")") && x.startsWith(" ")) s + x.substring(1)
            else if (s.isEmpty || s.endsWith(";")) x match {
              case leftEmptyString(i) =>
                s + i
              case _ => s + x
            }
            else s + x)
        case "" =>
          val s = sql.value
          StringTerm(if (s.endsWith(" ")) s.substring(0, s.length - 1) else s)
        case y if sql.value.endsWith("(") => sql + y
        case s if sql.value.endsWith(";") || sql.isEmpty => s match {
          case leftEmptyString(x) => sql + x
          case _ => sql + s
        }
        case x => sql + x
      }

    @tailrec
    def renderTree(asts: List[AST], context: RenderContext, sql: StringTerm): Term = asts match {
      case Nil => sql
      case Settings(key, children) :: tail =>
        val c = context.copy(settings = context.settings + (key -> render(children, context) _))
        renderTree(tail, c, sql)
      case head :: tail =>
        val rt = renderAST(head :: Nil, context)(root)
        rt.current match {
          //          case x@StringTerm(_) if sql.value.trim.isEmpty || sql.value.endsWith(";") => renderTree(tail, context, x.ltrim)
          case StringTerm(x) => renderTree(tail, context, trim(sql, x))
          case EmptyTerm => renderTree(tail, context, trim(sql, ""))
          case ListTerm(list) if list.forall(_.isInstanceOf[ValueTerm[_]]) => renderTree(tail, context, trim(sql, list.mkString(",")))
          case x: ValueTerm[_] => renderTree(tail, context, trim(sql, x.toString))
          case x => throw RenderException(s"can't render $x")
        }
    }

    @tailrec
    def renderAST(xs: List[AST], context: RenderContext)(term: RenderTerm[Term]): RenderTerm[Term] =
      if (term.current == EmptyTerm) term
      else xs match {
        case Nil => term
        case headAST :: tailAST =>
          val (ast, next): (List[AST], RenderTerm[Term]) = try {
            headAST match {
              case Text(x) =>
                val t = {
                  if (config.compress) StringTerm(x.replaceAll("""\s*([\(\),;])\s*""", "$1").replaceAll("\\s+", " "))
                  else StringTerm(x.replaceAll("""\n+\s*\n""", "\n"))
                }
                (tailAST, term.copy(current = t))

              case Empty => (tailAST, term)
              case Variable(children, value) =>
                val t =
                  if (value == "self") term
                  else {
                    val list = value.split("\\.").toList
                    val arg = list.head match {
                      case "root" => getTerm(list.tail, term.root)
                      case field => getTerm(list.tail, term(field).current)
                    }
                    arg match {
                      case x: ObjectTerm => term.copy(parent = term.parent + x, current = arg)
                      case _ => term.copy(current = arg)
                    }
                  }
                (children ::: tailAST, t)
              case FluentMap(children, body) => (tailAST, fluentMap(children, term, body, context))
              case AddEntry(Quotes(key), value) => (tailAST, addEntry(key, value,term,context))
              case Join(Quotes(start) :: Quotes(sep) :: Quotes(end) :: Nil) => (tailAST, term.join(start, sep, end))
              case Join(Quotes(sep) :: Nil) => (tailAST, term.join(sep).as[RenderTerm[Term]])
              case Replace(Quotes(regx) :: Quotes(replacement) :: Nil) => (tailAST, term.replace(regx, replacement))
              case Prefix(Quotes(start)) => (tailAST, term.join(start, "", ""))
              case Suffix(Quotes(end)) => (tailAST, term.join("", "", end))
              case Interpolation(value) =>
                val string = interpolationRegex.replaceAllIn(value, x => {
                  val t = x.group(1) match {
                    case "self" => term.current
                    case s =>
                      val list = s.split("\\.").toList
                      list.head match {
                        case "root" => getTerm(list.tail, term.root)
                        case "self" => getTerm(list.tail, term.current)
                        case field => getTerm(list.tail, term(field).current)
                      }

                  }
                  t match {
                    case StringTerm(s) => s
                    case NumberTerm(n) => n.toString
                    case BooleanTerm(b) => if (b) "true" else "false"
                    case EmptyTerm => ""
                    case y => throw ParseException(s"can't parse $y")
                  }
                })
                (tailAST, term.copy(current = StringTerm(string)))

              case Match(children) => (tailAST, matchCase(children, term, context))
              case Filter(condition) => (tailAST, filter(condition, term))
              case Group(Quotes(group)) => (tailAST, term.group(group))
              case Where(key) => (tailAST, term.copy(current = StringTerm(parseCondition(term, key, context))))
              case Distinct => (tailAST, term.distinct)
              case Index(Quotes(index)) => (tailAST, term.zipWithIndex(index.toInt))
              case Flatten => (tailAST, term.flatten)
              case Size => (tailAST, term.copy(current = NumberTerm(term.size)))
              case Quotes(x) => (tailAST, term.copy(current = StringTerm(x)))
              //              case Union(value) => (value :: tailAST, term.union(json))
              //term.union(renderAST(value :: Nil, json))
              case Union(children) => (tailAST, union(children, term, context))
              //ListTerm(children.map(x => renderAST(x :: Nil, term)))
              case Reference(x) =>
                (tailAST, term.copy(current = context.settings(x)(config)(term)))
              case _ => (tailAST, term)
            }
          }
          catch {
            case e: Exception =>
              println(xs.head, term)
              throw e
          }
          renderAST(ast, context)(next)
      }

    def parseCondition(condition: RenderTerm[Term], key: AST, context: RenderContext): String = {

      def parseValue(term: Term): String = term match {
        case StringTerm(s) => "'" + s.replaceAll("'", "\\\\\\\\'") + "'"
        case BooleanTerm(b) => if (b) "true" else "false"
        case NumberTerm(n) => n.toString
        case EmptyTerm => ""
      }

      def parse(condition: RenderTerm[Term]): String = {
        val ObjectTerm(query) = condition("query").current
        val op = query.head._1
        val list = query(op).as[ListTerm].value
        parseExpression(list, Nil).mkString("(", " " + symbol(op) + " ", ")")
      }

      @tailrec
      def parseExpression(list: List[Term], results: List[String]): List[String] = list match {
        case Nil => results
        case head :: tail =>
          val ObjectTerm(term) = head
          val string = if (term.contains("code")) {
            val StringTerm(code) = term("code")
            val values = term("values") match {
              case x: ListTerm => x.value.map(parseValue)
              case EmptyTerm => Nil
            }
            val value = values.headOption.getOrElse("")
            val StringTerm(operator) = term("operator")

            val in = interpolationRegex.replaceAllIn(symbol(operator.toLowerCase), x => {
              x.group(1) match {
                case "key" => if (code == "$" || key != Empty) renderAST(key :: Nil, context)(condition).current.toString else code
                case "tails" => values.mkString(",")
                case "value" => value
                case "values_comma" => values.mkString(",")
                case "values_pipe_wq" => values.map(_.replaceAll("'", "")).mkString("|")
                case v if v.startsWith("value.") => values(v.split("\\.")(1).toInt)
              }
            })
            in
          }
          else if (term.contains("query")) parse(RenderTerm(head))
          else ""

          parseExpression(tail, string :: results)
      }

      parse(condition)
    }


    def conditionOp(ast: AST, term: RenderTerm[Term]): Boolean = ast match {
      case Quotes("_") => true
      case Quotes(x) => term.current match {
        case StringTerm(s) => x == s
        case _ => false
      }
      case Condition(right, op, left) =>
        val r = renderAST(right :: Nil, context)(term).current
        val l = renderAST(left :: Nil, context)(term).current
        op match {
          case "=" => r match {
            case x: ValueTerm[_] => x.toString == l.toString
            case EmptyTerm => l.toString.isEmpty
            case _ => r == l
          }
          case "<>" => r match {
            case x: ValueTerm[_] => x.toString != l.toString
            case EmptyTerm => l.toString.nonEmpty
            case _ => r != l
          }
          case "has" => r.has(l)
          case "hasnot" => !r.has(l)
          case "is" => l match {
            case StringTerm("string") => r.isInstanceOf[StringTerm]
            case StringTerm("number") => r.isInstanceOf[NumberTerm]
            case StringTerm("boolean") => r.isInstanceOf[BooleanTerm]
            case StringTerm("array") => r.isInstanceOf[ListTerm]
            case StringTerm("object") => r.isInstanceOf[ObjectTerm]
            case StringTerm("empty") => r == EmptyTerm
            case _ => false
          }
          case "isnot" => l match {
            case StringTerm("string") => !r.isInstanceOf[StringTerm]
            case StringTerm("number") => !r.isInstanceOf[NumberTerm]
            case StringTerm("boolean") => !r.isInstanceOf[BooleanTerm]
            case StringTerm("array") => !r.isInstanceOf[ListTerm]
            case StringTerm("object") => !r.isInstanceOf[ObjectTerm]
            case StringTerm("empty") => r != EmptyTerm
            case _ => false
          }
        }

      case _ => false
    }


    @tailrec
    def matchCase(conditions: List[AST], term: RenderTerm[Term], context: RenderContext): RenderTerm[Term] = {

      conditions match {
        case Nil => term.copy(current = EmptyTerm)
        case Case(children, body) :: tail => if (children.forall(x => conditionOp(x, term))) renderAST(body :: Nil, context)(term) else matchCase(tail, term, context)
        case e => throw ParseException(s"can't parse $e")
      }
    }


    def filter(condition: AST, term: RenderTerm[Term]): RenderTerm[Term] = condition match {
      case c: Condition => term.copy(current = term.current.filter(x => conditionOp(condition, RenderTerm(x))))
      case e => throw ParseException(s"can't parse $e")
    }


    def fluentMap(children: List[AST], term: RenderTerm[Term], body: AST, context: RenderContext): RenderTerm[Term] = {
      val r = term.map {
        x =>
          val args = parseAssignment(children, term.copy(current = x), context)
          renderAST(body :: Nil, context)(args).current
      }.filter(!_.isEmpty)
      r
    }

    def parseAssignment(assignments: List[AST], term: RenderTerm[Term], context: RenderContext): RenderTerm[Term] =
      assignments match {
        case Nil => term
        case Assignment(value, Quotes(key)) :: tail =>
          val assign = ObjectTerm(Map(key -> renderAST(value :: Nil, context)(term).current))
          val t = (term + assign).copy(parent = term.parent + assign)
          parseAssignment(tail, t, context)
        case e => throw ParseException(s"can't parse $e")
      }

    def union(asts: List[AST], term: RenderTerm[Term], context: RenderContext): RenderTerm[Term] =
      asts.map(x => renderAST(x :: Nil, context)(term)).reduce(_ union _)

    def addEntry(key: String, value: AST,term: RenderTerm[Term],context: RenderContext): RenderTerm[Term] = {
      val x = renderAST(value::Nil,context)(term).current
      val r = term.add(key,x)
      r
    }

    val notEmpty = asts.filterNot {
      case x: Text => x.isEmpty
      case _ => false
    }

    notEmpty match {
      case Nil => EmptyTerm
      case head :: Nil => renderAST(head :: Nil, context)(root).current
      case _ => renderTree(asts, context, StringTerm(""))
    }

  }
}

case class RenderContext(settings: Map[String, Rule])

object RenderContext {
  val empty = RenderContext(Map.empty)
}