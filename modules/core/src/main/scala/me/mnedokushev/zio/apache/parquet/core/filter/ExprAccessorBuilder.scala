package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema.{ AccessorBuilder, Schema }

final class ExprAccessorBuilder(typeTags: Map[String, TypeTag[?]]) extends AccessorBuilder {

  override type Lens[F, S, A] = Column.Named[A, F]

  override type Prism[F, S, A] = Unit

  override type Traversal[S, A] = Unit

  override def makeLens[F, S, A](product: Schema.Record[S], term: Schema.Field[S, A]): Column.Named[A, F] = {
    val name             = term.name.toString
    implicit val typeTag = typeTags(name).asInstanceOf[TypeTag[A]]

    Column.Named[A, F](name)
  }

  override def makePrism[F, S, A](sum: Schema.Enum[S], term: Schema.Case[S, A]): Prism[F, S, A] =
    ()

  override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] =
    ()

}
