package me.mnedokushev.zio.apache.parquet.core.filter.internal

import me.mnedokushev.zio.apache.parquet.core.filter.{ CompiledPredicate, Predicate }
import org.apache.parquet.filter2.predicate.FilterPredicate

import scala.quoted.*

object SanitizeOptionalsMacro {

  // TODO: tests
  def sanitizeImpl[A: Type](predicate: Expr[Predicate[A]])(using Quotes): Expr[CompiledPredicate] = {
    import quotes.reflect.*

    // Example of a type representation of A type:
    // AndType(
    //   AndType(
    //     TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "scala")), "Predef"), "String"),
    //     AppliedType(
    //       TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Option"),
    //       List(
    //         TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Int")
    //       )
    //     )
    //   ),
    //   TypeRef(TermRef(ThisType(TypeRef(NoPrefix(), "<root>")), "scala"), "Int")
    // )
    // TODO: rewrite using limited stack for safety
    def containsOptionalValue(tpe: TypeRepr): Boolean =
      tpe match {
        case AndType(a, b)       =>
          containsOptionalValue(a) || containsOptionalValue(b)
        case AppliedType(tpe, _) =>
          containsOptionalValue(tpe)
        case TypeRef(_, name)    =>
          List("Option", "Some", "None").contains(name)
      }

    if (containsOptionalValue(TypeRepr.of[A]))
      report.errorAndAbort(
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate tree: ${predicate.show}
        """.stripMargin
      )
    else
      '{ _root_.me.mnedokushev.zio.apache.parquet.core.filter.Predicate.compile0($predicate) }

  }

}
