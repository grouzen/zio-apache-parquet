package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Lens
import me.mnedokushev.zio.apache.parquet.core.filter.CompiledPredicate
import me.mnedokushev.zio.apache.parquet.core.filter.internal.{ ColumnPathConcatMacro, SanitizeOptionalsMacro }

package object syntax {

  implicit class NullableColumnSyntax[F, S, A](val column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  def predicate[A](predicate: Predicate[A]): CompiledPredicate = macro SanitizeOptionalsMacro.sanitizeImpl[A]

  def concat[A, B, F](
    parent: Column[A],
    child: Column.Named[B, F]
  ): Column[B] = macro ColumnPathConcatMacro.concatImpl[A, B, F]

}
