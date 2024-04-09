package me.mnedokushev.zio.apache.parquet.core

import me.mnedokushev.zio.apache.parquet.core.filter.internal.CompilePredicateMacro
import org.apache.parquet.filter2.predicate.FilterPredicate

package object filter {

  extension[F, S, A](column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] =
    CompilePredicateMacro.compileImpl[A](predicate)

}
