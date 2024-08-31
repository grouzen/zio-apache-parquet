package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.{ Lens, Prism, Traversal }
import zio.schema._

trait Filter {

  type Columns

  val columns: Columns

}

object Filter {

  type Aux[Columns0] = Filter {
    type Columns = Columns0
  }

  def apply[A](implicit
    schema: Schema[A],
    typeTag: TypeTag[A]
  ): Filter.Aux[schema.Accessors[Lens, Prism, Traversal]] =
    new Filter {
      val accessorBuilder =
        new ExprAccessorBuilder(typeTag.asInstanceOf[TypeTag.Record[A]].columns)

      override type Columns =
        schema.Accessors[accessorBuilder.Lens, accessorBuilder.Prism, accessorBuilder.Traversal]

      override val columns: Columns =
        schema.makeAccessors(accessorBuilder)
    }

}
