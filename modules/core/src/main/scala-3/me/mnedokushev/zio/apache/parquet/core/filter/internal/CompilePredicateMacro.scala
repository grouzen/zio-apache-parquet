package me.mnedokushev.zio.apache.parquet.core.filter.internal

import scala.quoted._
import me.mnedokushev.zio.apache.parquet.core.filter.Predicate
import org.apache.parquet.filter2.predicate.FilterPredicate

object CompilePredicateMacro {

  inline def compileImpl[A](predicate: Predicate[A]): Either[String, FilterPredicate] = ${ compileImplImpl[A]('predicate) }

  def compileImplImpl[A: Type](predicate: Expr[Predicate[A]])(using Quotes): Expr[Either[String, FilterPredicate]] = {
    import quotes.reflect._

    val containsOptionalValue = predicate.asTerm match {
      case Inlined(_, _, Block(stats, _)) =>
        stats.exists {
          case Apply(TypeApply(Select(Ident("scala"), "Some"), _), _) =>
            true
          case Select(Ident("scala"), "None") =>
            true
          case _ =>
            false
        }
      case _ =>
        false
    }

    if (containsOptionalValue)
      report.errorAndAbort(
        s"""
           | The use of optional columns in filter predicate is prohibited. Please, use .nullable:
           |   column.nullable > 3
           | Predicate: ${predicate.show}
        """.stripMargin
      )
    else
      '{ _root_.me.mnedokushev.zio.apache.parquet.core.filter.Filter.compile[A]($predicate) }
  }

}