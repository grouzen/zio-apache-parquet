package me.mnedokushev.zio.apache.parquet.core.codec

import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

class ValueEncoderDeriver extends Deriver[ValueEncoder] {

  override def deriveRecord[A](
    record: Schema.Record[A],
    fields: => Chunk[Deriver.WrappedF[ValueEncoder, _]],
    summoned: => Option[ValueEncoder[A]]
  ): ValueEncoder[A] = ???

  override def deriveEnum[A](
    `enum`: Schema.Enum[A],
    cases: => Chunk[Deriver.WrappedF[ValueEncoder, _]],
    summoned: => Option[ValueEncoder[A]]
  ): ValueEncoder[A] = ???

  override def derivePrimitive[A](st: StandardType[A], summoned: => Option[ValueEncoder[A]]): ValueEncoder[A] = ???

  override def deriveOption[A](
    option: Schema.Optional[A],
    inner: => ValueEncoder[A],
    summoned: => Option[ValueEncoder[Option[A]]]
  ): ValueEncoder[Option[A]] = ???

  override def deriveSequence[C[_], A](
    sequence: Schema.Sequence[C[A], A, _],
    inner: => ValueEncoder[A],
    summoned: => Option[ValueEncoder[C[A]]]
  ): ValueEncoder[C[A]] = ???

  override def deriveMap[K, V](
    map: Schema.Map[K, V],
    key: => ValueEncoder[K],
    value: => ValueEncoder[V],
    summoned: => Option[ValueEncoder[Map[K, V]]]
  ): ValueEncoder[Map[K, V]] = ???

  override def deriveTransformedRecord[A, B](
    record: Schema.Record[A],
    transform: Schema.Transform[A, B, _],
    fields: => Chunk[Deriver.WrappedF[ValueEncoder, _]],
    summoned: => Option[ValueEncoder[B]]
  ): ValueEncoder[B] = ???

}
