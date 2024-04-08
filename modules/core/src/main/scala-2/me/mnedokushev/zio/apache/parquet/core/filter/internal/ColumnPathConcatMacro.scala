package me.mnedokushev.zio.apache.parquet.core.filter.internal

import scala.reflect.macros.blackbox
import me.mnedokushev.zio.apache.parquet.core.filter.Column

class ColumnPathConcatMacro(val c: blackbox.Context) extends MacroUtils(c) {
  import c.universe._

  def concatImpl[A, B](parent: Expr[Column[A]], child: Expr[Column[B]])(implicit ptt: c.WeakTypeTag[A]): Tree = {
    val childField   = getIdentName(child)
    val parentFields = ptt.tpe.members.collect {
      case p: TermSymbol if p.isCaseAccessor && !p.isMethod => p.name.toString.trim
    }.toList

    // childField.debugged()
    // parentFields.debugged()
    // parentFields.contains(childField).debugged()

    if (parentFields.exists(_ == childField)) {
      val pathTermName     = "path"
      val dotStringLiteral = "."
      val concatExpr       =
        q"${parent.tree}.${TermName(pathTermName)} + ${Literal(Constant(dotStringLiteral))} + ${child.tree}.${TermName(pathTermName)}"

      q"_root_.me.mnedokushev.zio.apache.parquet.core.filter.Column.Named($concatExpr)"
    } else
      c.abort(c.enclosingPosition, "parent/child relation is wrong")
  }

  def getIdentName[A](arg: Expr[A]): String =
    arg.tree match {
      case Ident(TermName(name)) =>
        name
      case _                     =>
        c.abort(c.enclosingPosition, s"Couldn't get a name of identifier: ${arg.tree}")
    }

}
