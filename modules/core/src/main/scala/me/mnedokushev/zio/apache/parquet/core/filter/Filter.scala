package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core._
import zio.schema._
// import zio.schema.validation.Validation

trait Filter[Columns0] {

  type Columns

  val columns: Columns0

}

object Filter {

  def columns[A](implicit
    schema: Schema[A],
    typeTag: TypeTag[A]
  ): schema.Accessors[Lens, Prism, Traversal] =
    new Filter[schema.Accessors[Lens, Prism, Traversal]] {

      // def optionless[A1](schema0: Schema[A1]): Schema[Any] =
      //   schema0 match {
      //     case Schema.CaseClass1(id, field, construct, annotations) =>
      //       Schema.CaseClass1(
      //         id,
      //         Schema.Field(
      //           field.name,<
      //           optionless(field.schema),
      //           field.annotations,
      //           field.validation.asInstanceOf[Validation[Any]],
      //           field.get.asInstanceOf[Any => Any],
      //           field.set.asInstanceOf[(Any, Any) => Any]
      //         ),
      //         construct,
      //         annotations
      //       )
      //     case s: Schema.Optional[_]                                =>
      //       optionless(s.schema)
      //     case s                                                    =>
      //       s.asInstanceOf[Schema[Any]]
      //   }

      val accessorBuilder =
        new ExprAccessorBuilder(typeTag.asInstanceOf[TypeTag.Record[A]].columns)

      override type Columns =
        schema.Accessors[accessorBuilder.Lens, accessorBuilder.Prism, accessorBuilder.Traversal]

      override val columns: Columns =
        schema.makeAccessors(accessorBuilder)

    }.columns

  def not[A](pred: Expr.Predicate[A]): Expr.Predicate[A] =
    Expr.Predicate.Unary(pred, Operator.Unary.Not[A]())

}
