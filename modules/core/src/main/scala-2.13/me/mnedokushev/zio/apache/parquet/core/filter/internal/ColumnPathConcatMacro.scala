package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.Column

import scala.reflect.macros.blackbox

class ColumnPathConcatMacro(val c: blackbox.Context) extends MacroUtils(c) {
  import c.universe._

  def concatImpl[A, B, F](parent: Expr[Column[A]], child: Expr[Column.Named[B, F]])(implicit
    ptt: c.WeakTypeTag[A],
    ftt: c.WeakTypeTag[F]
  ): Tree = {
    val childField   = getSingletonTypeName(ftt.tpe)
    val parentFields = ptt.tpe.members.collect {
      case p: TermSymbol if p.isCaseAccessor && !p.isMethod => p.name.toString.trim
    }.toList

    if (parentFields.exists(_ == childField)) {
      val pathTermName     = "path"
      val dotStringLiteral = "."
      val concatExpr       =
        q"${parent.tree}.${TermName(pathTermName)} + ${Literal(Constant(dotStringLiteral))} + ${child.tree}.${TermName(pathTermName)}"

      q"_root_.me.mnedokushev.zio.apache.parquet.core.filter.Column.Named($concatExpr)"
    } else
      c.abort(c.enclosingPosition, s"Parent column doesn't contain a column named $childField")
  }

  private def getSingletonTypeName(tpe: Type): String =
    tpe match {
      case ConstantType(Constant(name)) => name.toString
      case _                            => c.abort(c.enclosingPosition, s"Couldn't get a name of a singleton type ${showRaw(tpe)}")
    }

}
