package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema.AccessorBuilder
import zio.schema.Schema

class ExprAccessorBuilder extends AccessorBuilder {

  override type Lens[F, S, A] = Expr.Column[A]

  override type Prism[F, S, A] = Unit

  override type Traversal[S, A] = Unit

  override def makeLens[F, S, A](product: Schema.Record[S], term: Schema.Field[S, A]): Expr.Column[A] = {
    implicit val typeTag: TypeTag[A] = TypeTag.deriveTypeTag(term.schema).get

    Expr.Column(term.name.toString)
  }

  override def makePrism[F, S, A](sum: Schema.Enum[S], term: Schema.Case[S, A]): Prism[F, S, A] =
    ()

  override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] =
    ()

}
