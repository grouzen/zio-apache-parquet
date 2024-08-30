package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Lens
import me.mnedokushev.zio.apache.parquet.core.filter.internal.{ ColumnPathConcatMacro, SanitizeOptionalsMacro }

package object syntax extends Predicate.Syntax {

  extension [F, S, A](column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  inline def predicate[A](inline predicate: Predicate[A]): CompiledPredicate =
    ${ SanitizeOptionalsMacro.sanitizeImpl[A]('predicate) }

  inline def concat[A, B, F](inline parent: Column[A], inline child: Column.Named[B, F])(using
    ctt: TypeTag[B]
  ): Column[B] =
    ${ ColumnPathConcatMacro.concatImpl[A, B, F]('parent, 'child, 'ctt) }

}
