package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema._
import me.mnedokushev.zio.apache.parquet.core._

trait Filter[Columns0] {

  type Columns

  val columns: Columns0

}

object Filter {

  def columns[A](implicit schema: Schema.Record[A]): schema.Accessors[Lens, Prism, Traversal] =
    new Filter[schema.Accessors[Lens, Prism, Traversal]] {

      val accessorBuilder = new ExprAccessorBuilder

      override type Columns = schema.Accessors[accessorBuilder.Lens, accessorBuilder.Prism, accessorBuilder.Traversal]

      override val columns: Columns = schema.makeAccessors(accessorBuilder)

    }.columns

}
