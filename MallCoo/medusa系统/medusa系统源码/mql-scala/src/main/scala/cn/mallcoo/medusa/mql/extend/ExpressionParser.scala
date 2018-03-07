package cn.mallcoo.medusa.mql.extend

import cn.mallcoo.medusa.mql._

/**
  * Created by wm on 2017-09 14:05.
  */
object ExpressionParser {

  sealed trait Tree {
    val value: Option[String]
    val children: List[Tree]
    val level: Int

    def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree

    def +(tree: Tree): Tree = (value, children) match {
      case (Some(_), Nil) => this.create(children = tree :: Nil)
      case (None, Nil) => tree
      case (_, head :: tail) => this.create(children = head + tree :: tail)
    }

    def addNode(lv: Int): Tree =
      if (this.children.isEmpty) this match {
        case x: EmpytNode => Node(this.value, EmpytNode(lv) :: Nil, this.level)
        case _ => this.create(children = EmpytNode(lv) :: Nil)
      }
      else {
        val (head :: tail) = this.children
        if (this.level <= lv) this.create(children = head.addNode(lv) :: tail)
        else this.create(children = EmpytNode(lv + 1) :: this.children)
      }

    def appendNode(lv: Int): Tree =
      if (this.children.isEmpty) this
      else if (this.children.head.level < lv) this.create(children = children.head.appendNode(lv) :: children.tail)
      else if (this.children.head.level == lv) this.create(children = EmpytNode(lv) :: children)
      else this

    def rotate(tree: Tree, lv: Int): Tree = this.children match {
      case head :: tail if this.level < lv => this.create(children = head.rotate(tree, lv) :: tail)
      case _ if this.level == lv =>
        if (this.value.isEmpty) {
          BinaryNode(tree.value, EmpytNode(lv) :: this.children.headOption.getOrElse(this) :: Nil, level)
        } else {
          BinaryNode(tree.value, EmpytNode(lv) :: this :: Nil, level)
        }
    }

    def toStringWithoutAgg: String
  }

  case class EmpytNode(level: Int) extends Tree {
    override val value: Option[String] = None
    override val children: List[Tree] = Nil

    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = Leaf(value, level)

    override def toStringWithoutAgg: String = toString
  }

  case class FunctionNode(value: Option[String], children: List[Tree], level: Int) extends Tree {

    override def toString: String = {
      val x = if (children.isEmpty) "" else children.reverse.mkString("(", ", ", ")")
      value.getOrElse("None") + x
    }

    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = FunctionNode(value, children, level)

    override def toStringWithoutAgg: String = {
      val x = if (children.isEmpty) "" else children.reverse.map(x => x.toStringWithoutAgg).mkString("(", ", ", ")")
      value.getOrElse("None") + x
    }
  }

  case class BinaryNode(value: Option[String], children: List[Tree], level: Int) extends Tree {
    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = BinaryNode(value, children, level)

    override def toString: String = {
      val (left, right) = children match {
        case first :: second :: tail => (second, first)
        case head :: tail => ("_", head)
        case _ => ("_", "_")
      }
      value match {
        case Some("/") => s"udf.to_num($left/$right)"
        case Some(x) => s"($left$x$right)"
        case _ => s"($left _ $right)"
      }
    }

    override def toStringWithoutAgg: String = {
      val (left, right) = children match {
        case first :: second :: tail => (second.toStringWithoutAgg, first.toStringWithoutAgg)
        case head :: tail => ("_", head.toStringWithoutAgg)
        case _ => ("_", "_")
      }
      value match {
        case Some("/") => s"udf.to_num($left/$right)"
        case Some(x) => s"($left$x$right)"
        case _ => s"($left _ $right)"
      }
    }
  }

  case class AggregateNode(value: Option[String], children: List[Tree], level: Int) extends Tree {
    override def toString: String = {
      val x = if (children.isEmpty) "" else children.mkString("(", ", ", ")")

      value.getOrElse("None") + x
    }

    override def toStringWithoutAgg: String = children.head.toStringWithoutAgg

    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = AggregateNode(value, children, level)
  }

  case class Leaf(value: Option[String], level: Int) extends Tree {
    override def toString: String = value.getOrElse("None")

    override val children: List[Tree] = Nil

    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = Node(value, children, level)

    override def toStringWithoutAgg: String = toString
  }

  case class Node(value: Option[String], children: List[Tree], level: Int) extends Tree {
    override def toString: String ={
      val x = if (children.isEmpty) "" else children.reverse.mkString("(", ", ", ")")
      value.getOrElse("") + x
    }

    override def create(value: Option[String] = value, children: List[Tree] = children, level: Int = level): Tree = Node(value, children, level)

    override def toStringWithoutAgg: String = {
      val x = if (children.isEmpty) "" else children.reverse.map(x => x.toStringWithoutAgg).mkString("(", ", ", ")")
      value.getOrElse("") + x
    }
  }

  def parse(tokens: List[String], tree: Tree, level: Int): Tree = tokens match {
    case Nil => tree
    case token :: tail =>
      val n = token.toLowerCase.trim match {
        case "(" => (tree.addNode(level + 1), level + 1)
        case ")" => (tree, level - 1)
        case "," => (tree.appendNode(level), level)
        case x if x == "greatest" => (tree + FunctionNode(Some(x), Nil, level), level)
        case x if x == "avg" || x == "sum" => (tree + AggregateNode(Some(x), Nil, level), level)
        case x if x == "+" || x == "-" || x == "*" || x == "/" =>
          (tree.rotate(FunctionNode(Some(x), Nil, level), level), level)
        case x => (tree + Leaf(Some(x), level), level)
      }
      parse(tail, n._1, n._2)
  }

  def render(expression: String): Tree = {
    val tokens = expressionRegx.findAllIn(expression).toList
    parse(tokens, FunctionNode(None, Nil, 0), 0)
  }

  case class DecomposeTree(i: Int, j: Int, root: String, parts: List[String])

  def decompose(tree: Tree, parts: DecomposeTree = DecomposeTree(1, 0, "", Nil)): DecomposeTree = tree match {
    case a: AggregateNode => parts.copy(j = parts.j + 1, root = s"${parts.root}x${parts.i}${parts.j}", parts = a.value.get + "(" + a.children.head.toString + ")" + s" x${parts.i}${parts.j}" :: parts.parts)
    case b: BinaryNode =>
      val x :: y :: _ = b.children.reverse
      val right = decompose(x, parts.copy(j = parts.j + 1))
      decompose(y, right.copy(root = right.root + b.value.get))
    case f: FunctionNode =>
      val d = parts.copy(root = parts.root + f.value.get + "(")
      val result = f.children.reverse.foldLeft(d) { (x, y) =>
        val result = decompose(y, x)
        if (result.root.endsWith("(")) result
        else result.copy(root = "," + result.root)
      }
      result.copy(root = result.root + ")")

    case f: Leaf => decompose(EmpytNode(f.level),parts.copy(root = parts.root + f.value.get))

    case _ => parts
  }

}
