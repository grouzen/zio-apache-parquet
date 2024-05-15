package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.{ Column, TypeTag }

import scala.quoted.*

object ColumnPathConcatMacro {

  def concatImpl[A: Type, B: Type, F: Type](
    parent: Expr[Column[A]],
    child: Expr[Column.Named[B, F]],
    childTypeTag: Expr[TypeTag[B]]
  )(using
    Quotes
  ): Expr[Column[B]] = {
    import quotes.reflect.*

    val childField   = TypeRepr.of[F] match {
      case ConstantType(StringConstant(name)) =>
        name
      case tpe                                =>
        report.errorAndAbort(s"Couldn't get a name of a singleton type $tpe")
    }
    val parentFields = TypeRepr.of[A].typeSymbol.caseFields.map(_.name)

    if (parentFields.contains(childField)) {
      val concatExpr = '{ ${ parent }.path + "." + ${ child }.path }

      '{ me.mnedokushev.zio.apache.parquet.core.filter.Column.Named[B, F]($concatExpr)($childTypeTag) }
    } else
      report.errorAndAbort(s"Parent column doesn't contain a column named '$childField'")
  }

}
