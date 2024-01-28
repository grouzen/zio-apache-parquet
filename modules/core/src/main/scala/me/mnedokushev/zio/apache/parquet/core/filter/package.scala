package me.mnedokushev.zio.apache.parquet.core

import me.mnedokushev.zio.apache.parquet.core.filter.internal.CompilePredicateMacro
import org.apache.parquet.filter2.predicate.FilterPredicate

package object filter {

  implicit class NullableColumnSyntax[S, A](column: Lens[String, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column[A] =
      Column[A](column.path)
  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] =
    macro CompilePredicateMacro.compileImpl[A]

}
