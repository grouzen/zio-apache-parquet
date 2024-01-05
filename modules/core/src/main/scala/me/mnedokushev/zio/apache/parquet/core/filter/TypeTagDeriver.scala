package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema.Deriver
import zio.Chunk
import zio.schema.Schema
import zio.schema.StandardType
import zio.schema.Schema

object TypeTagDeriver {

  val default: Deriver[TypeTag] = new Deriver[TypeTag] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[TypeTag, _]],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] =
      TypeTag.Record(
        record.fields
          .map(_.name.toString)
          .zip(fields.map(_.unwrap))
          .toMap
      )

    override def deriveEnum[A](
      enum: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[TypeTag, _]],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] = 
      TypeTag.dummy[A]

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] =
      st match {
        case StandardType.StringType => TypeTag.TString
        case StandardType.BoolType   => TypeTag.TBoolean
        case StandardType.ByteType   => TypeTag.TByte
        case StandardType.ShortType  => TypeTag.TShort
        case StandardType.IntType    => TypeTag.TInt
        case StandardType.LongType   => TypeTag.TLong
        case _                       => TypeTag.dummy[A]
      }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => TypeTag[A],
      summoned: => Option[TypeTag[Option[A]]]
    ): TypeTag[Option[A]] = 
      TypeTag.dummy[Option[A]]

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => TypeTag[A],
      summoned: => Option[TypeTag[C[A]]]
    ): TypeTag[C[A]] = 
      TypeTag.dummy[C[A]]

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => TypeTag[K],
      value: => TypeTag[V],
      summoned: => Option[TypeTag[Map[K, V]]]
    ): TypeTag[Map[K, V]] = 
      TypeTag.dummy[Map[K, V]]

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[TypeTag, _]],
      summoned: => Option[TypeTag[B]]
    ): TypeTag[B] = 
      TypeTag.dummy[B]

  }.cached

  val summoned: Deriver[TypeTag] = default.autoAcceptSummoned

}
