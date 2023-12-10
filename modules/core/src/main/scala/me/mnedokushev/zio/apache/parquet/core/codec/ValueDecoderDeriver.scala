package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.Value.{ GroupValue, PrimitiveValue }
import zio._
import zio.schema._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

object ValueDecoderDeriver {

  val default: Deriver[ValueDecoder] = new Deriver[ValueDecoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueDecoder, _]],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = new ValueDecoder[A] {
      override def decode(value: Value): A =
        value match {
          case GroupValue.RecordValue(values) =>
            Unsafe.unsafe { implicit unsafe =>
              record.construct(
                Chunk
                  .fromIterable(values.values)
                  .zip(fields.map(_.unwrap))
                  .map { case (v, decoder) =>
                    decoder.decode(v)
                  }
              ) match {
                case Right(v)     =>
                  v
                case Left(reason) =>
                  throw DecoderError(s"Couldn't decode $value: $reason")
              }
            }

          case other =>
            throw DecoderError(s"Couldn't decode $other, it must be of type RecordValue")
        }

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[ValueDecoder, _]],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = new ValueDecoder[A] {
      override def decode(value: Value): A =
        (st, value) match {
          case (StandardType.StringType, PrimitiveValue.BinaryValue(v)) =>
            new String(v.getBytes, StandardCharsets.UTF_8)
          case (StandardType.BoolType, PrimitiveValue.BooleanValue(v))  =>
            v
          case (StandardType.ByteType, PrimitiveValue.Int32Value(v))    =>
            v.toByte
          case (StandardType.ShortType, PrimitiveValue.Int32Value(v))   =>
            v.toShort
          case (StandardType.IntType, PrimitiveValue.Int32Value(v))     =>
            v
          case (StandardType.LongType, PrimitiveValue.Int64Value(v))    =>
            v
          case (StandardType.FloatType, PrimitiveValue.FloatValue(v))   =>
            v
          case (StandardType.DoubleType, PrimitiveValue.DoubleValue(v)) =>
            v
          case (StandardType.BinaryType, PrimitiveValue.BinaryValue(v)) =>
            Chunk.fromArray(v.getBytes)
          case (StandardType.CharType, PrimitiveValue.Int32Value(v))    =>
            v.toChar
          case (StandardType.UUIDType, PrimitiveValue.BinaryValue(v))   =>
            val bb = ByteBuffer.wrap(v.getBytes)
            new UUID(bb.getLong, bb.getLong)
          case (other, _)                                               =>
            throw DecoderError(s"Unsupported ZIO Schema StandartType $other")
        }
    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueDecoder[A],
      summoned: => Option[ValueDecoder[Option[A]]]
    ): ValueDecoder[Option[A]] = new ValueDecoder[Option[A]] {
      override def decode(value: Value): Option[A] =
        value match {
          case Value.NullValue =>
            None
          case _               =>
            Some(inner.decode(value))
        }

    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => ValueDecoder[A],
      summoned: => Option[ValueDecoder[C[A]]]
    ): ValueDecoder[C[A]] = new ValueDecoder[C[A]] {
      override def decode(value: Value): C[A] =
        value match {
          case GroupValue.ListValue(values) =>
            sequence.fromChunk(values.map(inner.decode))
          case other                        =>
            throw DecoderError(s"Couldn't decode $other, it must be of type ListValue")
        }
    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => ValueDecoder[K],
      value: => ValueDecoder[V],
      summoned: => Option[ValueDecoder[Map[K, V]]]
    ): ValueDecoder[Map[K, V]] = new ValueDecoder[Map[K, V]] {
      override def decode(value0: Value): Map[K, V] =
        value0 match {
          case GroupValue.MapValue(values) =>
            values.map { case (k, v) =>
              key.decode(k) -> value.decode(v)
            }
          case other                       =>
            throw DecoderError(s"Couldn't decode $other, it must be of type MapValue")
        }
    }

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[ValueDecoder, _]],
      summoned: => Option[ValueDecoder[B]]
    ): ValueDecoder[B] = ???

  }.cached

  def summoned: Deriver[ValueDecoder] =
    default.autoAcceptSummoned

}
