package cn.mallcoo.medusa.mql.test.core

import cn.mallcoo.medusa.mql.test.core.AST._

import scala.annotation.tailrec
import scala.collection.immutable.::

/**
  * Created by wm on 2017/6/27.
  */
case class ParserContext(root: List[AST], stack: List[AST]) {
  def top(ast: AST) = stack match {
    case Nil => this.copy(stack = ast :: Nil)
    case head :: tail => this.copy(stack = ast :: tail)
  }

  def push(ast: AST) = this.copy(stack = ast :: stack)

//  @tailrec
//  private def append(context: ParserContext): ParserContext = {
//    context.stack match {
//      case Nil => context
//      case Chain(_)::Nil => context
//      case head :: Nil => context
//      case Empty :: tail => context.copy(stack = tail)
//      case head :: Empty :: Nil => context
//      case first :: Empty :: Empty :: tail => append(context.copy(stack = first :: Empty :: tail))
//      case first :: Empty :: second :: tail => context.copy(stack = second + first :: tail)
//      case first :: second :: Empty :: tail => context.copy(stack = second + first :: Empty :: tail)
//      case first :: second :: tail => append(context.copy(stack = second + first :: tail))
//    }
//    context
//  }

//  @tailrec
  private def pop(context: ParserContext): ParserContext = {
//    context.stack match {
//      case Nil => context
//      case Chain(_)::Nil => context
//      case head :: Nil => addRoot(head).copy(stack = Nil)
//      case Empty :: tail => context.copy(stack = tail)
//      case head :: Empty :: Nil => addRoot(head).copy(stack = Nil)
//      case first :: Empty :: Empty :: tail => pop(context.copy(stack = first :: Empty :: tail))
//      case first :: Empty :: second :: tail => context.copy(stack = second + first :: tail)
//      case first :: second :: tail => pop(context.copy(stack = second + first :: tail))
//    }
    context
  }

  def popAll: ParserContext = this//stack match {
//    case Nil => this
//    case Text(x) :: tail if x.trim.isEmpty => this.copy(stack = tail).popAll
//    case Empty :: tail => this.copy(stack = tail).popAll
//    case head :: tail => this.copy(stack = tail, root = head :: root).popAll
  //}

  private def addRoot(ast: AST): ParserContext = this
//    if (root.isEmpty) this.copy(root = ast :: Nil)
//    else (root.head, ast) match {
//      case (Text(a), Text(b)) => this.copy(root = Text(a + b) :: root.tail)
//      case _ => this.copy(root = ast :: root)
//    }

//  def unappend: ParserContext = stack match {
//    case Nil => this
//    case head :: tail => head.unappend match {
//      case (Empty, x) => this.top(x)
//      case (x, y) => this.top(y).push(x)
//    }
//  }

//  def append: ParserContext = append(this)

//  def pop: ParserContext = pop(pop(this))
def pop = this.copy(root = stack.head::root,stack = Nil)

  def merge: ParserContext = stack match {
    case Chain.empty::Chain.empty::tail => this.copy(stack =Chain.empty::tail).merge
    case Chain(_)::_ => this
    case first :: second :: tail => this.append.merge
    case _ => throw ParseException(s"$this can not merge")
  }

  def ++ :ParserContext = stack match {
    case first :: second :: tail => this.copy(stack = second + first :: tail)
    case _ => throw ParseException(s"$this can not merge")
  }

  def append = this ++

  //  @tailrec
  final def appendAll: ParserContext = this //stack match {
//    case head :: Nil => this
//    case Nil => this
//    case head :: Empty :: Nil => this.copy(stack = head :: Nil).appendAll
//    case head :: tail => append(this).appendAll
//  }

  @tailrec
  final def ltrim :ParserContext = root match {
    case Nil => this
    case Text(x) :: tail if x.trim.isEmpty => this.copy(root = tail).ltrim
    case x => this
  }

  def reverse :ParserContext = this.copy(root = root.reverse)


  def skip = stack match {
    case Nil => this
    case head::Nil => this
    case head::second::tail => this.copy(stack = head::tail)
  }
}

object ParserContext {
  val empty = new ParserContext(Nil, Nil)
}