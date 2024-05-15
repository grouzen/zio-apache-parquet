package me.mnedokushev.zio.apache.parquet.core


import me.mnedokushev.zio.apache.parquet.core.filter.internal.ColumnPathConcatMacro

package object filter {

  extension [F, S, A](column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  inline def concat[A, B, F](inline parent: Column[A], inline child: Column.Named[B, F])(using
    ctt: TypeTag[B]
  ): Column[B] =
    ${ ColumnPathConcatMacro.concatImpl[A, B, F]('parent, 'child, 'ctt) }

}
