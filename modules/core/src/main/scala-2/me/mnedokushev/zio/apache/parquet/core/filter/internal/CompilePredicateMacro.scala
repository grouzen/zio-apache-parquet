package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.Predicate

import scala.reflect.macros.blackbox

class CompilePredicateMacro(val c: blackbox.Context) {
  import c.universe._

  def compileImpl[A](predicate: Expr[Predicate[A]]): Tree = {
    val containsOptionalValue = predicate.tree.exists {
      case q"scala.Some" =>
        true
      case q"scala.None" =>
        true
      case _             =>
        false
    }

    if (containsOptionalValue)
      c.abort(
        c.enclosingPosition,
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate: ${predicate.tree}
        """.stripMargin
      )
    else
      q"_root_.me.mnedokushev.zio.apache.parquet.core.filter.Filter.compile($predicate)"
  }

}
