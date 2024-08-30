package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.Predicate

import scala.reflect.macros.blackbox

class SanitizeOptionalsMacro(val c: blackbox.Context) extends MacroUtils(c) {
  import c.universe._

  def sanitizeImpl[A](predicate: Expr[Predicate[A]])(ptt: c.WeakTypeTag[A]): Tree = {

    // Example of a tree for A type:
    // RefinedType(
    //   List(
    //     RefinedType(
    //       List(
    //         TypeRef(
    //           ThisType(java.lang),
    //           java.lang.String,
    //           List()
    //         ),
    //         TypeRef(
    //           ThisType(scala),
    //           scala.Option,
    //           List(
    //             TypeRef(
    //               ThisType(scala),
    //               scala.Int,
    //               List()
    //             )
    //           )
    //         )
    //       ),
    //       Scope()
    //     ),
    //     TypeRef(ThisType(scala), scala.Int, List())
    //   ),
    //   Scope()
    // )
    // TODO: rewrite using limited stack for safety
    def containsOptionalValue(tpe: Type): Boolean =
      tpe match {
        case RefinedType(tpes, _) =>
          tpes.exists(containsOptionalValue)
        case TypeRef(_, sym, _)   =>
          List("scala.Option", "scala.Some", "scala.None").contains(sym.fullName)
        case _                    =>
          false
      }

    if (containsOptionalValue(ptt.tpe))
      c.abort(
        c.enclosingPosition,
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate: ${predicate.tree}
        """.stripMargin
      )
    else
      q"_root_.me.mnedokushev.zio.apache.parquet.core.filter.Predicate.compile0($predicate)"
  }

}
