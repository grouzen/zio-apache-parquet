package me.mnedokushev.zio.apache.parquet.core

package object filter {

  implicit class NullableColumnSyntax[S, A](column: Lens[String, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column[A] =
      Column[A](column.path)
  }

}
