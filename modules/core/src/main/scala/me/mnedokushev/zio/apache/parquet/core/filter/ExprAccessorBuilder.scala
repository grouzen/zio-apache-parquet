package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema.AccessorBuilder
import zio.schema.Schema

final class ExprAccessorBuilder(typeTags: Map[String, TypeTag[_]]) extends AccessorBuilder {

  override type Lens[F, S, A] = Expr.Column[A]

  override type Prism[F, S, A] = Unit

  override type Traversal[S, A] = Unit

  override def makeLens[F, S, A](product: Schema.Record[S], term: Schema.Field[S, A]): Expr.Column[A] = {
    // val typeTag = TypeTag.deriveTypeTag(term.schema).get
    // val typeTag = Derive.derive[TypeTag, A](TypeTagDeriver.default)(term.schema)
    val typeTag = typeTags.get(term.name.toString).map(_.asInstanceOf[TypeTag[A]])

    val r: Expr.Column[A] = typeTag match {
      case Some(t: TypeTag.EqNotEq[A]) =>
        Expr.ColumnEqNotEq[A](term.name.toString)(t)
      case Some(t: TypeTag.LtGt[A])    =>
        Expr.ColumnLtGt[A](term.name.toString)(t)
      case _                           =>
        Expr.ColumnDummy[A](term.name.toString)(TypeTag.dummy[A])
    }

    r
  }

  override def makePrism[F, S, A](sum: Schema.Enum[S], term: Schema.Case[S, A]): Prism[F, S, A] =
    ()

  override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] =
    ()

}
