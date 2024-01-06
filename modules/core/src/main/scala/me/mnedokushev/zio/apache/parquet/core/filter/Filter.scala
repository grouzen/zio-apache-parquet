package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core._
import zio.schema._

trait Filter[Columns0] {

  type Columns

  val columns: Columns0

}

object Filter {

  def columns[A](implicit
    schema: Schema.Record[A],
    typeTag: TypeTag[A]
  ): schema.Accessors[Lens, Prism, Traversal] =
    new Filter[schema.Accessors[Lens, Prism, Traversal]] {

      val accessorBuilder = new ExprAccessorBuilder(typeTag.asInstanceOf[TypeTag.Record[A]].columns)

      override type Columns = schema.Accessors[accessorBuilder.Lens, accessorBuilder.Prism, accessorBuilder.Traversal]

      override val columns: Columns = schema.makeAccessors(accessorBuilder)

    }.columns

  def not[A](pred: Expr.Predicate[A]): Expr.Predicate[A] =
    Expr.Predicate.Unary(pred, Operator.Unary.Not[A]())

}
