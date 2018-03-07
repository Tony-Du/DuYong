package cn.mallcoo.medusa.mql.test.core

import cn.mallcoo.medusa.mql.test.core.AST.Empty

sealed trait AST {
  def +(ast: AST): AST

  def unary_- : (AST, AST)
}

trait Leaf extends AST {
  override def +(ast: AST): AST = this

  override def unary_- : (AST, AST) = (Empty, this)

}

object AST {

  case object Empty extends Leaf

  case class Text(value: String) extends Leaf {
    val isEmpty = value.trim.isEmpty

    override def toString: String = value
  }

  case class Variable(value: String) extends Leaf {
    override def toString: String = s"#$value#"
  }

  case class Chain(children: List[AST]) extends AST {
    override def +(ast: AST): Chain = this.copy(children = (children.toBuffer += ast).toList)

    override def unary_- : (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))

    override def toString = children match {
      case Nil => "EmptyChain"
      case head :: Nil => head.toString
      case _ => children.mkString(".")
    }

    def head = children.headOption.getOrElse(Empty)

    def tail = children match {
      case Nil => Chain(Nil)
      case h :: t => Chain(t)
    }
  }

  case object Chain {
    val empty = new Chain(Nil)
  }

  case class Assignment(key: String, value: Chain) extends AST {

    override def +(ast: AST) = this.copy(value = value + ast)

    override def unary_- : (AST, AST) = (value.head, this.copy(value = value.tail))

    override def toString: String = s"$key -> $value"
  }

  case class FluentMap(assignments: List[Assignment], body: Chain) extends AST {
    override def +(ast: AST): AST = ast match {
      case x: Assignment => this.copy(assignments = (assignments.toBuffer += x).toList)
      case y: Chain => this.copy(body = y)
      case e => throw ParseException(s"can not add $e to map node")
    }

    override def unary_- : (AST, AST) = (body, this.copy(body = Chain(Nil)))

    override def toString: String = assignments match {
      case Nil => s"Map($body)"
      case _ => s"Map { ${assignments.mkString(",")} => $body }"
    }
  }

  case class Union(children: List[Chain]) extends AST {
    override def +(ast: AST): AST = ast match {
      case x: Chain => this.copy(children = (children.toBuffer += x).toList)
      case e => throw ParseException(s"can not add $e to union node")
    }

    override def unary_- : (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))

    override def toString: String = children.mkString("Union(", ",", ")")
  }

  case class Join(sep: Chain, start: Chain, end: Chain) extends AST {
    private def isEmpty(chain: Chain) =  chain.children match {
      case Nil => Chain.empty
      case Quotes("") :: Nil => Chain.empty
      case _ => chain
    }

    override def +(ast: AST): AST = ast match {
      case x: Chain => (sep, start, end) match {
        case (Chain.empty, _, _) => this.copy(sep = x)
        case (_, Chain.empty, _) => this.copy(start = x)
        case (_, _, Chain.empty) => this.copy(end = x)
        case _ => throw ParseException(s"join node has too manay args")
      }
      case e => throw ParseException(s"can not add $e to join node")
    }

    override def unary_- : (AST, AST) = this match {
      case Join(Chain.empty, Chain.empty, Chain.empty) => (Empty, this)
      case Join(x, Chain.empty, Chain.empty) => (x, this.copy(sep = Chain.empty))
      case Join(_, x, Chain.empty) => (x, this.copy(start = Chain.empty))
      case Join(_, _, x) => (x, this.copy(end = Chain.empty))
      case _ => (Empty, Join(Chain.empty, Chain.empty, Chain.empty))

    }

    override def toString: String = (isEmpty(sep), isEmpty(start), isEmpty(end)) match {
      case (Chain.empty,Chain.empty,_) => s"Suffix($end)"
      case (Chain.empty,_,Chain.empty) => s"Prefix($start)"
      case (_,Chain.empty,Chain.empty) => s"Join($sep)"
      case _ => s"Join($sep, $start, $end)"
    }
  }

  case class Match(cases: List[Case]) extends AST {
    override def +(ast: AST): AST = ast match {
      case x: Case => this.copy(cases = (cases.toBuffer += x).toList)
      case e => throw ParseException(s"can not add $e to match node")
    }

    override def unary_- : (AST, AST) = (cases.headOption.getOrElse(Empty), this.copy(cases = cases.tail))

    override def toString: String = s"Match { ${cases.mkString(",")} }"
  }

  case class Case(children: List[Condition], body: Chain) extends AST {
    override def +(ast: AST): AST = ast match {
      case c: Condition => this.copy(children = (children.toBuffer += c).toList)
      case x: Chain => this.copy(body = x)
      case e => throw ParseException(s"can not add $e to case node")
    }

    override def unary_- : (AST, AST) = (body, this.copy(body = Chain.empty))

    override def toString: String = s"${children.mkString(" & ")} => $body"
  }

  case class Group(value: Chain) extends AST {

    override def +(ast: AST): AST = ast match {
      case x: Chain => Group(x)
      case e => throw ParseException(s"can not add $e to group node")
    }

    override def unary_- : (AST, AST) = (value, Group(Chain.empty))
  }

  case class Condition(right: Chain, op: String, left: Chain) extends AST {
    override def unary_- : (AST, AST) = (left, this.copy(left = Chain.empty))

    override def +(ast: AST): AST = ast match {
      case x: Chain => this.copy(left = x)
      case e => throw ParseException(s"can not add $e to condition node")
    }

    override def toString: String = (right,left) match {
      case (Chain.empty,Chain.empty) => op
      case (_,Chain.empty) => right + " " + op
      case (Chain.empty,_) => op + " " + left
      case _ => right + " " + op + " " + left
    }

  }

  case class Settings(key: String, children: List[AST]) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unary_- : (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))

    override def toString: String = s"$key <- ${children.mkString(" ")}"
  }

  case class Filter(value: Chain) extends AST {
    override def +(ast: AST): AST = ast match {
      case x: Chain => Filter(x)
      case e => throw ParseException(s"can not add $e to filter node")
    }

    override def unary_- : (AST, AST) = (value, Filter(Chain.empty))

    override def toString: String = value match {
      case Chain.empty => "Filter()"
      case _ => s"Filter($value)"
    }
  }

  case class Interpolation(value: String) extends Leaf {
    override def toString: String = "`" + value + "`"
  }

  case class Reference(value: String) extends Leaf {
    override def toString: String = "@" + value
  }

  case class Quotes(value: String) extends Leaf {
    override def toString: String = s"'$value'"
  }

  case object Distinct extends Leaf

  case object Flatten extends Leaf

  case class Index(index: Int) extends Leaf

  case object Size extends Leaf


  case object End extends Leaf {
    override def toString: String = ";"
  }

}




