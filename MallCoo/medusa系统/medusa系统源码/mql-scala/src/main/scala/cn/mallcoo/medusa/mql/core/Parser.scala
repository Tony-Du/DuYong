package cn.mallcoo.medusa.mql.core

import cn.mallcoo.medusa.mql._
import cn.mallcoo.medusa.mql.adt.AST._
import cn.mallcoo.medusa.mql.adt.{AST, ParserContext}

import scala.annotation.tailrec

/**
  * Created by whimmy on 17/06/02.
  */
case class Parser(settings: Map[String, String]) {

  val globalSetting = settings.map { x =>
    val reference = referenceRegex.findAllMatchIn(x._2).map(_.group(1)).toSet[String]
    val inner = innerSettings.findAllMatchIn(x._2).map(_.group(1)).toSet
    x._1 -> (parse(x._2), (reference -- inner).toList)
  }

  def tokenize(string: String) = termRegex.findAllIn(string).toList

  @tailrec
  private def findNextEmpty(list: List[AST]): List[AST] = list match {
    case Nil => Nil
    case Empty :: tail => list
    case head :: tail => findNextEmpty(tail)
  }

  def parse(string: String): List[AST] = {

    @tailrec
    def parseAST(xs: List[String], context: ParserContext): ParserContext = xs match {
      case Nil => context.pop
      case x :: tail if x.startsWith("{{{") =>
        val key = x.replaceAll("""\{\{\{""", "").replaceAll("<-", "").trim
        val (ys, c) = parseToken(tail, ParserContext.empty)
        parseAST(ys, context.copy(root = Settings(key, c.popAll.root.reverse) :: context.root))
      case _ =>
        val (ys, c) = parseToken(xs, context)
        parseAST(ys, c)
    }

    @tailrec
    def parseToken(tokens: List[String], context: ParserContext): (List[String], ParserContext) = tokens match {
      case Nil => (Nil, context)
      case "}}}" :: x => (x, context)
      case x :: _ if x.startsWith("{{{") => (tokens, context.pop)
      case token :: tailToken =>
        val next = context.stack match {
          case Nil =>
            if (token == "{{") context.push(Empty)
            else context.root.headOption match {
              case Some(Settings(_, _)) if token.trim.isEmpty => context
              case None if token.trim.isEmpty => context
              case _ => context.push(Empty).push(Text(token))
            }
          case Text(x) :: Empty :: Nil if token != "{{" && token != "}}" => context.top(Text(x + token))
          case (x@Assignment(_, Empty)) :: _ if token.trim.nonEmpty => context.top(x + Quotes(token))
          case current :: tail => token match {
            case "{{" => context.pop.push(Empty)
            case "}}" => context.pop
            case x if x.trim.isEmpty => context
            case "<-" => context.push(Empty)
            case x if x.startsWith("@") => context.push(Reference(x.substring(1)))
            case x if x.startsWith("#") && x.endsWith("#") => context.push(Variable(Nil, x.substring(1, x.length - 1)))
            case x if x.startsWith("`") && x.endsWith("`") => context.push(Interpolation(x.substring(1, x.length - 1)))
            case x if x.startsWith("'") => context.push(Quotes(x.substring(1, x.length - 1)))
            case "_" => context.push(Quotes("_"))
            case "." if current.isInstanceOf[Case] => context.unappend
            case "(" | "{" => context.push(Empty)
            case ")" | "}" => tail match {
              case (head: Case) :: _ => context.append.append.append
              case (_:Condition) :: _ :: Filter(_) :: _ => context.append.append.append
              case _ => context.append.append
//                findNextEmpty(tail) match {
//                  case Empty :: first :: xs => first match {
//                    case _: Filter if tail.head == Empty => context.top(Case(List(current), Empty))
//                    case _: Filter =>
//                      val c = context.append
//                      c.stack.head match {
//                        case _: Case => c
//                        case _ => c.top(Case(List(c.stack.head), Empty))
//                      }
//                    case _: FluentMap => context.append.push(Empty)
//                    case _ => context.append.append
//                  }
//                  case _ => context.append.append
//                }
            }
            case "," => tail match {
              case (head: Case) :: _ => context.append.append.push(Empty)
              case _ => context.append.push(Empty)
            }
            case "->" => current match {
              case x: FluentMap =>
                val c = context.unappend
                c.top(Empty).push(Assignment(c.stack.head, Empty))
              case _ => context.top(Assignment(current, Empty))
            }
            case "=>" => findNextEmpty(tail) match {
              case Nil => context
              case Empty :: first :: xs => first match {
                case _: Match if tail.head == Empty => context.top(Case(List(current), Empty))
                case _: Filter if tail.head == Empty => context.top(Case(List(current), Empty))
                case _: Match =>
                  val c = context.append
                  c.stack.head match {
                    case _: Case => c
                    case _ => c.top(Case(List(c.stack.head), Empty))
                  }
                case _: Filter =>
                  val c = context.append
                  c.stack.head match {
                    case _: Case => c
                    case _ => c.top(Case(List(c.stack.head), Empty))
                  }
                case _: FluentMap => context.append.push(Empty)
              }
              case _ => context
            }
            case "&" if tail.head == Empty => context.top(Case(List(current), Empty))
            case "&" =>
              val c = context.append
              c.stack.head match {
                case _: Case => c
                case _: Filter => c
                case _ => c.top(Case(List(c.stack.head), Empty))
              }

            case "map" => context.push(FluentMap(Nil, Empty))
            //            case "union" => context.push(Union(Empty))
            case "replace" => context.push(Replace(Nil))
            case "add" => context.push(AddEntry(Empty,Empty))
            case "join" => context.push(Join(Nil))
            case "prefix" => context.push(Prefix(Empty))
            case "suffix" => context.push(Suffix(Empty))
            case "match" => context.push(Match(Nil))
            case "filter" => context.push(Filter(Empty))
            case "group" => context.push(Group(Empty))
            case "where" => context.push(Where(Empty))
            case "distinct" => context.push(Distinct).append
            case "index" => context.push(Index(Empty))
            case "flatten" => context.push(Flatten).append
            case "size" => context.push(Size).append
            case "union" => context.push(Union(Nil))
            case "has" => context.top(Condition(current, "has", Empty))
            case "hasnot" => context.top(Condition(current, "hasnot", Empty))
            case "is" => context.top(Condition(current, "is", Empty))
            case "isnot" => context.top(Condition(current, "isnot", Empty))
            case "<>" => context.top(Condition(current, "<>", Empty))
            case "=" => context.top(Condition(current, "=", Empty))
            case _ => context
          }
        }
        parseToken(tailToken, next)
    }

    parseAST(tokenize(string), ParserContext(Nil, Nil)).popAll.root.reverse
  }
}
