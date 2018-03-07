package cn.mallcoo.medusa

import cn.mallcoo.medusa.mql.adt.Term
import cn.mallcoo.medusa.mql.adt.Term.RenderTerm
import cn.mallcoo.medusa.mql.core.RenderConfig

/**
  * Created by whimmy on 17/06/08.
  */
package object mql extends {

  type Rule = RenderConfig => RenderTerm[Term] => Term

  val termRegex = """@\w+|index\(\d+\)|#[\d\w\\.]+#|\w+|\{\{\{\s*\w+\s*<-|\}\}\}|\{\{|\}\}|`[^`]*?`|'[^']*?'|\(|\)|\{|\}|\.|=>|->|,|\S|\s+""".r

  val interpolationRegex ="""#(.*?)#""".r

  val connectStringRegex = """^[\(\),; ].*""".r

  val referenceRegex = """@([\w_]+)""".r

  val innerSettings = """\{\{\{\s*([\w_]+)\s*<-""".r

  val rightEmptyString = """(.*)\s+$""".r

  val leftEmptyString = """^\s+(.*)""".r

  val replacementRegex = """#\d+|.""".r

  val expressionRegx = """\w+|\(|\)|\*|/|\+|\-|,""".r

}
