package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.filter.internal.CompilePredicateMacro
import me.mnedokushev.zio.apache.parquet.core.filter.CompiledPredicate
import org.apache.parquet.filter2.predicate.FilterPredicate

package object syntax {

  implicit class NullableColumnSyntax[F, S, A](val column: Lens[F, S, Option[A]]) {
    def nullable(implicit typeTag: TypeTag[A]): Column.Named[A, column.Identity] =
      Column.Named(column.path)
  }

  def compile[A](predicate: Predicate[A]): CompiledPredicate = 
    macro CompilePredicateMacro.compileImpl[A]

  // NOTE: no compile-time check on parent/child columns relation due to lack of singleton types in scala 2.12
  def concat[A, B: TypeTag, F](parent: Column[A], child: Column.Named[B, F]): Column[B] =
    Column.Named[B, F](s"${parent.path}.${child.path}")

}
