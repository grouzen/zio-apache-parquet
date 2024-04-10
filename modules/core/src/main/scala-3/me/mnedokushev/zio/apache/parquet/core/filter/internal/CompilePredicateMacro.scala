package me.mnedokushev.zio.apache.parquet.core.filter.internal

import scala.quoted.*
import me.mnedokushev.zio.apache.parquet.core.filter.Predicate
import org.apache.parquet.filter2.predicate.FilterPredicate

object CompilePredicateMacro {

  // TODO: tests
  def compileImpl[A: Type](predicate: Expr[Predicate[A]])(using Quotes): Expr[Either[String, FilterPredicate]] = {
    import quotes.reflect.*

    def containsOptionalValue(term: Term): Boolean =
      term match {
        case Inlined(_, _, term0)       =>
          containsOptionalValue(term0)
        case Apply(fun, args)           =>
          containsOptionalValue(fun) || args.forall(containsOptionalValue)
        case TypeApply(fun, _)          =>
          containsOptionalValue(fun)
        case Select(Ident("Some"), _)   =>
          true
        case Ident("None")              =>
          true
        case Select(Ident("Option"), _) =>
          true
        case _                          =>
          false
      }

    if (containsOptionalValue(predicate.asTerm))
      report.errorAndAbort(
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate tree: ${predicate.show}
        """.stripMargin
      )
    else
      '{ _root_.me.mnedokushev.zio.apache.parquet.core.filter.Filter.compile[A]($predicate) }
  }

}
