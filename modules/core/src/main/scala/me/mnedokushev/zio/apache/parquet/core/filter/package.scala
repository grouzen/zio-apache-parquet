package me.mnedokushev.zio.apache.parquet.core

import me.mnedokushev.zio.apache.parquet.core.filter.internal.CompilePredicateMacro
import me.mnedokushev.zio.apache.parquet.core.filter.internal.ColumnPathConcatMacro
import org.apache.parquet.filter2.predicate.FilterPredicate

package object filter {

  implicit class NullableColumnSyntax[F, S, A](val column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  def compile[A](predicate: Predicate[A]): Either[String, FilterPredicate] =
    macro CompilePredicateMacro.compileImpl[A]

  def concat[A, B](parent: Column[A], child: Column[B]): Column[B] =
    macro ColumnPathConcatMacro.concatImpl[A, B]

}
