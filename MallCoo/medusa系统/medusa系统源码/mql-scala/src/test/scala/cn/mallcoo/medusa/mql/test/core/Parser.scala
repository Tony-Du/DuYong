package cn.mallcoo.medusa.mql.test.core

import cn.mallcoo.medusa.mql.test.core.AST._

import scala.annotation.tailrec

/**
  * Created by whimmy on 17/06/02.
  */
case class Parser(settings: Map[String, String]) {

  //  val globalSetting = settings.map { x =>
  //    val reference = referenceRegex.findAllMatchIn(x._2).map(_.group(1)).toSet[String]
  //    val inner = innerSettings.findAllMatchIn(x._2).map(_.group(1)).toSet
  //    x._1 -> (parse(x._2), (reference -- inner).toList)
  //  }

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
      case Nil => context
      case x :: tail if x.startsWith("{{{") =>
        val key = x.replaceAll("""\{\{\{""", "").replaceAll("<-", "").trim
        val (ys, c) = parseToken(tail, ParserContext.empty)
        parseAST(ys, context.copy(root = Settings(key, c.ltrim.reverse.ltrim.root) :: context.root))
      case _ =>
        val (ys, c) = parseToken(xs, context)
        parseAST(ys, c)
    }

    @tailrec
    def parseToken(tokens: List[String], context: ParserContext): (List[String], ParserContext) = tokens match {
      case Nil => (Nil, context)
      case "}}}" :: x => (x, context)
      case x :: _ if x.startsWith("{{{") => (tokens, context)
      case ";" :: x => (x, context.copy(root = End :: context.root))
      case token :: tailToken =>
        val next = (context.stack, token) match {
          case (Nil, "{{") => context.push(Chain(Nil))
          case (Nil, _) => context.root match {
            case Settings(_, _) :: _ if token.trim.isEmpty => context
            case Text(x) :: tail => context.copy(root = Text(x + token) :: tail)
            case Nil if token.trim.isEmpty => context
            case x => context.copy(root = Text(token) :: x)
          }
          case ((x@Assignment(_, Chain.empty)) :: _, t) if t.trim.nonEmpty => context.top(x + Quotes(token))
          case (_, "{{") => context.push(Chain(Nil))
          case (_, "}}") => context.pop
          case (_, t) if t.trim.isEmpty => context
          case (_, "<-") => context.push(Empty)
          case (_, t) if t.startsWith("@") => context.push(Reference(t.substring(1)))
          case (_, t) if t.startsWith("index(") && t.endsWith(")") => context.push(Index(t.substring(6, t.length - 1).toInt)).append
          case (Chain(_) :: _, t) if t.startsWith("#") && t.endsWith("#") => context.push(Variable(t.substring(1, t.length - 1))).merge
          case (_, t) if t.startsWith("#") && t.endsWith("#") => context.push(Variable(t.substring(1, t.length - 1)))
          case (_, t) if t.startsWith("`") && t.endsWith("`") => context.push(Interpolation(t.substring(1, t.length - 1)))
          case (_, t) if t.startsWith("'") && t.endsWith("'") => context.push(Quotes(t.substring(1, t.length - 1)))

          //            case "." if current.isInstanceOf[Case] => context.unappend
          case (_, "(" | "{") => context.push(Chain.empty)
          case (_ :: Case(_, _) :: _, ")" | "}") => context
          case (_, ")" | "}") => context.merge.append.merge
          //          case (_ :: Case(_, _) :: _, ",") => context.push(Chain.empty)

          //          case current :: tail => token match {

          //            case "->" => current match {
          //              case x: FluentMap =>
          //                val c = context.unappend
          //                c.top(Empty).push(Assignment(c.stack.head, Chain.empty))
          //              case _ => context.top(Assignment(current, Chain.empty))
          //            }
          //            case "=>" => findNextEmpty(tail) match {
          //              case Nil => context
          //              case Empty :: first :: xs => first match {
          //                case _: Match if tail.head == Empty => context.top(Case(List(current), Chain.empty))
          //                case _: Match =>
          //                  val c = context.append
          //                  c.stack.head match {
          //                    case _: Case => c
          //                    case _ => c.top(Case(List(c.stack.head), Empty))
          //                  }
          //                case _: FluentMap => context.append.push(Empty)
          //              }
          //              case _ => context
          //            }

          case (_, ",") =>
            val c = context.merge.append
            c.stack match {
              case Assignment(_, _) :: Chain.empty :: FluentMap(_, _) :: _ => c.skip.append.push(Chain.empty)
              case Case(_, _) :: Match(_) :: tail => c.append.push(Case(Nil, Chain.empty)).push(Chain.empty)
              case _ => c.push(Chain.empty)
            }

          case (x :: Chain.empty :: Case(_, _) :: tail, "=>") => context.copy(stack = Case(Condition(Chain(Variable("self") :: Nil), "=", Chain(x :: Nil)) :: Nil, Chain.empty) :: tail).push(Chain(Nil))

          case (_, "=>") =>
            val c = context.merge.append
            c.stack match {
              case Assignment(_, _) :: Chain.empty :: FluentMap(_, _) :: _ => c.skip.append.push(Chain.empty)
              case Condition(_, _, _) :: Case(_, _) :: tail => c.append.push(Chain(Nil))
              case x => throw ParseException(s"canot parse $x =>")
            }

          case (_, t) if t.endsWith("->") => context.push(Assignment(t.substring(0, t.length - 2).trim, Chain.empty)).push(Chain.empty)

          //          case (_::Assignment(_,_)::Chain.empty::FluentMap(_,_)::_,"=>") => context.merge.append.skip.append.push(Chain.empty)
          //          case (_ :: Chain(_) :: Condition(_, _, _) :: Case(_, _) :: tail, "=>") => context.merge.append.append.push(Chain(Nil))

          case (_ :: Chain(_) :: Condition(_, _, _) :: Case(_, _) :: tail, "&") => context.merge.append.append.push(Chain(Nil))
          //          case (_ :: Chain(_) :: Case(_, _) :: Match(_) :: tail, ",") => context.merge.append.append.push(Case(Nil, Chain.empty)).push(Chain.empty)
          //            case "&" =>
          //              val c = context.append
          //              c.stack.head match {
          //                case _: Case => c
          //                case _ => c.top(Case(List(c.stack.head), Empty))
          //              }

          case (_, "map") => context.push(FluentMap(Nil, Chain.empty))
          case (_, "join") => context.push(Join(Chain.empty, Chain.empty, Chain.empty))
          case (_, "prefix") => context.push(Join(Chain(Quotes("") :: Nil), Chain.empty, Chain.empty))
          case (_, "suffix") => context.push(Join(Chain(Quotes("") :: Nil), Chain(Quotes("") :: Nil), Chain.empty))
          case (_, "match") => context.push(Match(Nil)).push(Case(Nil, Chain.empty))
          case (_, "group") => context.push(Group(Chain.empty))
          case (_, "filter") => context.push(Filter(Chain.empty))
          case (_, "distinct") => context.push(Distinct).append
          case (_, "flatten") => context.push(Flatten).append
          case (_, "size") => context.push(Size).append
          case (_, "union") => context.push(Union(Nil))
          case (_, "_") => context.top(Condition(Chain.empty, "_", Chain.empty)).push(Chain.empty).push(Chain.empty)
          case (Chain(x) :: _, "has") => context.top(Condition(Chain(x), "has", Chain.empty)).push(Chain.empty)
          case (Chain(x) :: _, "is") => context.top(Condition(Chain(x), "is", Chain.empty)).push(Chain.empty)
          case (Chain(x) :: _, "isnot") => context.top(Condition(Chain(x), "isnot", Chain.empty)).push(Chain.empty)
          case (Chain(x) :: _, "<>") => context.top(Condition(Chain(x), "<>", Chain.empty)).push(Chain.empty)
          case (Chain(x) :: _, "=") => context.top(Condition(Chain(x), "=", Chain.empty)).push(Chain.empty)

          //          case (_, ",") => context.merge.append.push(Chain.empty)
          case _ => context
        }

        parseToken(tailToken, next)
    }

    parseAST(tokenize(string), ParserContext(Nil, Nil)).ltrim.reverse.ltrim.root
  }
}
