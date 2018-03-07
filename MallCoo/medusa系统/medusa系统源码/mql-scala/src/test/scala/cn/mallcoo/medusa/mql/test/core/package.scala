package cn.mallcoo.medusa.mql.test

import cn.mallcoo.medusa.mql.test.core.Term.RenderTerm

/**
  * Created by wm on 2017/6/27.
  */
package object core {
  type Rule = RenderConfig => RenderTerm[Term] => Term

  val termRegex = """@\w+|index\(\d+\)|\w+\s*->|#[\d\w\\.]+#|\w+|\{\{\{\s*\w+\s*<-|\}\}\}|\{\{|\}\}|`[^`]*?`|'[^']*?'|\(|\)|\{|\}|\.|=>|,|\S|\s+""".r

  val interpolationRegex ="""#(.*?)#""".r

  val connectStringRegex = """^[\(\),; ].*""".r

  val referenceRegex = """@([\w_]+)""".r

  val innerSettings = """\{\{\{\s*([\w_]+)\s*<-""".r

  val rightEmptyString = """(.*)\s+$""".r

  val leftEmptyString = """^\s+(.*)""".r
}
