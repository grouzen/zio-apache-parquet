package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value.{ GroupValue, PrimitiveValue }
import me.mnedokushev.zio.apache.parquet.core.{ DECIMAL_SCALE, MICROS_FACTOR, MILLIS_PER_DAY, Value }
import zio._
import zio.schema._

import java.math.{ BigDecimal, BigInteger }
import java.nio.{ ByteBuffer, ByteOrder }
import java.time.{
  DayOfWeek,
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  Month,
  MonthDay,
  OffsetDateTime,
  OffsetTime,
  Period,
  Year,
  YearMonth,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.{ Currency, UUID }

object ValueDecoderDeriver {

  val default: Deriver[ValueDecoder] = new Deriver[ValueDecoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueDecoder, ?]],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = new ValueDecoder[A] {
      override def decode(value: Value): A =
        value match {
          case GroupValue.RecordValue(values) =>
            Unsafe.unsafe { implicit unsafe =>
              record.construct(
                Chunk
                  .fromIterable(record.fields.map(f => values(f.name)))
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
      cases: => Chunk[Deriver.WrappedF[ValueDecoder, ?]],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = new ValueDecoder[A] {
      override def decode(value: Value): A = {
        val casesMap = `enum`.cases.map { case0 =>
          case0.id -> case0.schema.asInstanceOf[Schema.CaseClass0[A]].defaultConstruct()
        }.toMap

        derivePrimitive(StandardType.StringType, summoned = None).map { case0 =>
          casesMap.getOrElse(case0, throw DecoderError(s"Failed to decode enum for id $case0"))
        }.decode(value)
      }
    }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueDecoder[A]]
    ): ValueDecoder[A] = new ValueDecoder[A] {

      private def localTime(v: Int) =
        LocalTime.ofNanoOfDay(v * MICROS_FACTOR)

      private def localDateTime(v: Long) = {
        val epochDay  = v / MILLIS_PER_DAY
        val nanoOfDay = (v - (epochDay * MILLIS_PER_DAY)) * MICROS_FACTOR

        LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay))
      }

      override def decode(value: Value): A =
        (st, value) match {
          case (StandardType.StringType, PrimitiveValue.BinaryValue(v))        =>
            v.toStringUsingUTF8
          case (StandardType.BoolType, PrimitiveValue.BooleanValue(v))         =>
            v
          case (StandardType.ByteType, PrimitiveValue.Int32Value(v))           =>
            v.toByte
          case (StandardType.ShortType, PrimitiveValue.Int32Value(v))          =>
            v.toShort
          case (StandardType.IntType, PrimitiveValue.Int32Value(v))            =>
            v
          case (StandardType.LongType, PrimitiveValue.Int64Value(v))           =>
            v
          case (StandardType.FloatType, PrimitiveValue.FloatValue(v))          =>
            v
          case (StandardType.DoubleType, PrimitiveValue.DoubleValue(v))        =>
            v
          case (StandardType.BinaryType, PrimitiveValue.BinaryValue(v))        =>
            Chunk.fromArray(v.getBytes)
          case (StandardType.CharType, PrimitiveValue.Int32Value(v))           =>
            v.toChar
          case (StandardType.UUIDType, PrimitiveValue.BinaryValue(v))          =>
            val bb = ByteBuffer.wrap(v.getBytes)

            new UUID(bb.getLong, bb.getLong)
          case (StandardType.CurrencyType, PrimitiveValue.BinaryValue(v))      =>
            Currency.getInstance(v.toStringUsingUTF8)
          case (StandardType.BigDecimalType, PrimitiveValue.Int64Value(v))     =>
            BigDecimal.valueOf(v, DECIMAL_SCALE)
          case (StandardType.BigIntegerType, PrimitiveValue.BinaryValue(v))    =>
            new BigInteger(v.getBytes)
          case (StandardType.DayOfWeekType, PrimitiveValue.Int32Value(v))      =>
            DayOfWeek.of(v)
          case (StandardType.MonthType, PrimitiveValue.Int32Value(v))          =>
            Month.of(v)
          case (StandardType.MonthDayType, PrimitiveValue.BinaryValue(v))      =>
            val bb = ByteBuffer.wrap(v.getBytes).order(ByteOrder.LITTLE_ENDIAN)

            MonthDay.of(bb.get.toInt, bb.get.toInt)
          case (StandardType.PeriodType, PrimitiveValue.BinaryValue(v))        =>
            val bb = ByteBuffer.wrap(v.getBytes).order(ByteOrder.LITTLE_ENDIAN)

            Period.of(bb.getInt, bb.getInt, bb.getInt)
          case (StandardType.YearType, PrimitiveValue.Int32Value(v))           =>
            Year.of(v)
          case (StandardType.YearMonthType, PrimitiveValue.BinaryValue(v))     =>
            val bb = ByteBuffer.wrap(v.getBytes).order(ByteOrder.LITTLE_ENDIAN)

            YearMonth.of(bb.getShort.toInt, bb.getShort.toInt)
          case (StandardType.ZoneIdType, PrimitiveValue.BinaryValue(v))        =>
            ZoneId.of(v.toStringUsingUTF8)
          case (StandardType.ZoneOffsetType, PrimitiveValue.BinaryValue(v))    =>
            ZoneOffset.of(v.toStringUsingUTF8)
          case (StandardType.DurationType, PrimitiveValue.Int64Value(v))       =>
            Duration.fromMillis(v)
          case (StandardType.InstantType, PrimitiveValue.Int64Value(v))        =>
            Instant.ofEpochMilli(v)
          case (StandardType.LocalDateType, PrimitiveValue.Int32Value(v))      =>
            LocalDate.ofEpochDay(v.toLong)
          case (StandardType.LocalTimeType, PrimitiveValue.Int32Value(v))      =>
            localTime(v)
          case (StandardType.LocalDateTimeType, PrimitiveValue.Int64Value(v))  =>
            localDateTime(v)
          case (StandardType.OffsetTimeType, PrimitiveValue.Int32Value(v))     =>
            OffsetTime.of(localTime(v), ZoneOffset.UTC)
          case (StandardType.OffsetDateTimeType, PrimitiveValue.Int64Value(v)) =>
            OffsetDateTime.of(localDateTime(v), ZoneOffset.UTC)
          case (StandardType.ZonedDateTimeType, PrimitiveValue.Int64Value(v))  =>
            ZonedDateTime.of(localDateTime(v), ZoneId.of("Z"))
          case (other, _)                                                      =>
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
      sequence: Schema.Sequence[C[A], A, ?],
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
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[ValueDecoder, ?]],
      summoned: => Option[ValueDecoder[B]]
    ): ValueDecoder[B] = summoned.getOrElse {
      new ValueDecoder[B] {
        override def decode(value: Value): B =
          value match {
            case GroupValue.RecordValue(values) =>
              Unsafe.unsafe { implicit unsafe =>
                record.construct(
                  Chunk
                    .fromIterable(record.fields.map(f => values(f.name)))
                    .zip(fields.map(_.unwrap))
                    .map { case (v, decoder) =>
                      decoder.decode(v)
                    }
                ).flatMap(transform.f) match {
                  case Right(v)     => v
                  case Left(reason) =>
                    throw DecoderError(s"Couldn't decode $value: $reason")
                }
              }

            case other =>
              throw DecoderError(s"Couldn't decode $other, it must be of type RecordValue")
          }
      }
    }
  }.cached

  def summoned: Deriver[ValueDecoder] =
    default.autoAcceptSummoned

}
