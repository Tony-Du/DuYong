package cn.mallcoo.medusa.mql.adt

import cn.mallcoo.medusa.mql.adt.AST.{Empty, Text}
import cn.mallcoo.medusa.mql.adt.Term.NumberTerm

import scala.annotation.tailrec

/**
  * Created by wm on 2017/5/24.
  */

case class ParseException(msg: String) extends RuntimeException(msg)

sealed trait AST {
  val children: List[AST]

  def +(ast: AST): AST

  def unappend: (AST, AST)
}

trait Leaf extends AST {
  override val children: List[AST] = Nil

  override def +(ast: AST): AST = this

  override def unappend: (AST, AST) = (Empty, this)

}

object AST {

  case object Empty extends Leaf

  case class Text(value: String) extends Leaf {
    val isEmpty = value.trim.isEmpty
  }

  case class Variable(children: List[AST], value: String) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))
  }

  case class Assignment(value: AST, key: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST) = this.copy(key = ast)

    override def unappend: (AST, AST) = (key, Assignment(value, Empty))
  }

  case class FluentMap(children: List[AST], body: AST) extends AST {
    override def +(ast: AST): AST = ast match {
      case x: Assignment => this.copy(children = (children.toBuffer += x).toList)
      case y => this.copy(body = y)
    }

    override def unappend: (AST, AST) = (body, this.copy(body = Empty))
  }

  case class Union(children: List[AST]) extends AST {

    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))
  }

  case class AddEntry(quotes: AST, value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = ast match {
      case x: Quotes => this.copy(quotes = x)
      case y: AST => this.copy(value = y)
    }

    override def unappend: (AST, AST) = (quotes, this.copy(quotes = Empty))
  }


  case class Interpolation(value: String) extends Leaf

  case class Join(children: List[AST]) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))
  }

  case class Replace(children: List[AST]) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))
  }

  case class Prefix(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = this.copy(value = ast)

    override def unappend: (AST, AST) = (value, Prefix(Empty))
  }

  case class Suffix(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = this.copy(value = ast)

    override def unappend: (AST, AST) = (value, Prefix(Empty))
  }

  case class Filter(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = ast match {
      case c: Condition => this.copy(value = c)
      case _ => Filter(Empty)
    }

    override def unappend: (AST, AST) = (value, Filter(Empty))
  }

  case class Match(children: List[AST]) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))

  }

  case class Case(children: List[AST], body: AST) extends AST {
    override def +(ast: AST): AST = ast match {
      case c: Condition => this.copy(children = (children.toBuffer += ast).toList)
      case _ => this.copy(body = ast)
    }

    override def unappend: (AST, AST) = (body, this.copy(body = Empty))

  }

  case class Group(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = Group(ast)

    override def unappend: (AST, AST) = (value, Group(Empty))
  }

  case class Quotes(value: String) extends Leaf

  case class Condition(right: AST, op: String, left: AST) extends AST {
    override val children: List[AST] = Nil

    override def unappend: (AST, AST) = (left, this.copy(left = Empty))

    override def +(ast: AST): AST = this.copy(left = ast)

  }

  case class Reference(value: String) extends Leaf

  case class Settings(key: String, children: List[AST]) extends AST {
    override def +(ast: AST): AST = this.copy(children = (children.toBuffer += ast).toList)

    override def unappend: (AST, AST) = (children.headOption.getOrElse(Empty), this.copy(children = children.tail))
  }

  //  case object Filter extends Leaf

  case class Where(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = Where(ast)

    override def unappend: (AST, AST) = (value, Where(Empty))
  }

  case object Distinct extends Leaf

  case object Flatten extends Leaf

  case class Index(value: AST) extends AST {
    override val children: List[AST] = Nil

    override def +(ast: AST): AST = Index(ast)

    override def unappend: (AST, AST) = (value, Index(Empty))
  }

  case object Size extends Leaf

}

case class ParserContext(root: List[AST], stack: List[AST]) {
  def top(ast: AST) = stack match {
    case Nil => this.copy(stack = ast :: Nil)
    case head :: tail => this.copy(stack = ast :: tail)
  }

  def push(ast: AST) = this.copy(stack = ast :: stack)

  @tailrec
  private def append(context: ParserContext): ParserContext = {
    context.stack match {
      case Nil => context
      case head :: Nil => context
      case Empty :: tail => context.copy(stack = tail)
      case head :: Empty :: Nil => context
      case first :: Empty :: Empty :: tail => append(context.copy(stack = first :: Empty :: tail))
      case first :: Empty :: second :: tail => context.copy(stack = second + first :: tail)
      case first :: second :: Empty :: tail => context.copy(stack = second + first :: Empty :: tail)
      case first :: second :: tail => append(context.copy(stack = second + first :: tail))
    }
  }

  @tailrec
  private def pop(context: ParserContext): ParserContext = {
    context.stack match {
      case Nil => context
      case head :: Nil => addRoot(head).copy(stack = Nil)
      case Empty :: tail => context.copy(stack = tail)
      case head :: Empty :: Nil => addRoot(head).copy(stack = Nil)
      case first :: Empty :: Empty :: tail => pop(context.copy(stack = first :: Empty :: tail))
      case first :: Empty :: second :: tail => context.copy(stack = second + first :: tail)
      case first :: second :: tail => pop(context.copy(stack = second + first :: tail))
    }
  }

  def popAll: ParserContext = stack match {
    case Nil => this
    case Text(x) :: tail if x.trim.isEmpty => this.copy(stack = tail).popAll
    case Empty :: tail => this.copy(stack = tail).popAll
    case head :: tail => this.copy(stack = tail, root = head :: root).popAll
  }

  private def addRoot(ast: AST): ParserContext =
    if (root.isEmpty) this.copy(root = ast :: Nil)
    else (root.head, ast) match {
      case (Text(a), Text(b)) => this.copy(root = Text(a + b) :: root.tail)
      case _ => this.copy(root = ast :: root)
    }

  def unappend: ParserContext = stack match {
    case Nil => this
    case head :: tail => head.unappend match {
      case (Empty, x) => this.top(x)
      case (x, y) => this.top(y).push(x)
    }
  }

  def append: ParserContext = append(this)

  def pop: ParserContext = pop(pop(this))

  @tailrec
  final def appendAll: ParserContext = stack match {
    case head :: Nil => this
    case Nil => this
    case head :: Empty :: Nil => this.copy(stack = head :: Nil).appendAll
    case head :: tail => append(this).appendAll
  }
}

object ParserContext {
  val empty = new ParserContext(Nil, Nil)
}
