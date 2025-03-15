package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

import java.math.{ BigDecimal, BigInteger }
import java.time.{
  DayOfWeek,
  Duration,
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
import java.util.{Currency, UUID}

object ValueEncoderDeriver {

  val default: Deriver[ValueEncoder] = new Deriver[ValueEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueEncoder, ?]],
      summoned: => Option[ValueEncoder[A]]
    ): ValueEncoder[A] = new ValueEncoder[A] {

      private def enc[A1](v: A, field: Schema.Field[A, A1], encoder: ValueEncoder[?]) =
        encoder.asInstanceOf[ValueEncoder[A1]].encode(field.get(v))

      override def encode(value: A): Value =
        Value.record(
          record.fields
            .zip(fields.map(_.unwrap))
            .map { case (field, encoder) =>
              field.name -> enc(value, field, encoder)
            }
            .toMap
        )
    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[ValueEncoder, ?]],
      summoned: => Option[ValueEncoder[A]]
    ): ValueEncoder[A] = new ValueEncoder[A] {
      override def encode(value: A): Value = {
        val casesMap = `enum`.cases.map { case0 =>
          case0.schema.asInstanceOf[Schema.CaseClass0[A]].defaultConstruct() -> case0.id
        }.toMap

        derivePrimitive(StandardType.StringType, summoned = None)
          .contramap[A] { case0 =>
            casesMap.getOrElse(case0, throw EncoderError(s"Failed to encode enum for value $case0"))
          }
          .encode(value)
      }
    }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueEncoder[A]]
    ): ValueEncoder[A] =
      new ValueEncoder[A] {
        override def encode(value: A): Value =
          (st, value) match {
            case (StandardType.StringType, v: String)                 =>
              Value.string(v)
            case (StandardType.BoolType, v: Boolean)                  =>
              Value.boolean(v)
            case (StandardType.ByteType, v: Byte)                     =>
              Value.int(v.toInt)
            case (StandardType.ShortType, v: Short)                   =>
              Value.short(v)
            case (StandardType.IntType, v: Int)                       =>
              Value.int(v)
            case (StandardType.LongType, v: Long)                     =>
              Value.long(v)
            case (StandardType.FloatType, v: Float)                   =>
              Value.float(v)
            case (StandardType.DoubleType, v: Double)                 =>
              Value.double(v)
            case (StandardType.BinaryType, v: Chunk[_])               =>
              Value.binary(v.asInstanceOf[Chunk[Byte]])
            case (StandardType.CharType, v: Char)                     =>
              Value.char(v)
            case (StandardType.UUIDType, v: UUID)                     =>
              Value.uuid(v)
            case (StandardType.CurrencyType, v: Currency)             =>
              Value.currency(v)
            case (StandardType.BigDecimalType, v: BigDecimal)         =>
              Value.bigDecimal(v)
            case (StandardType.BigIntegerType, v: BigInteger)         =>
              Value.bigInteger(v)
            case (StandardType.DayOfWeekType, v: DayOfWeek)           =>
              Value.dayOfWeek(v)
            case (StandardType.MonthType, v: Month)                   =>
              Value.month(v)
            case (StandardType.MonthDayType, v: MonthDay)             =>
              Value.monthDay(v)
            case (StandardType.PeriodType, v: Period)                 =>
              Value.period(v)
            case (StandardType.YearType, v: Year)                     =>
              Value.year(v)
            case (StandardType.YearMonthType, v: YearMonth)           =>
              Value.yearMonth(v)
            case (StandardType.ZoneIdType, v: ZoneId)                 =>
              Value.zoneId(v)
            case (StandardType.ZoneOffsetType, v: ZoneOffset)         =>
              Value.zoneOffset(v)
            case (StandardType.DurationType, v: Duration)             =>
              Value.duration(v)
            case (StandardType.InstantType, v: Instant)               =>
              Value.instant(v)
            case (StandardType.LocalDateType, v: LocalDate)           =>
              Value.localDate(v)
            case (StandardType.LocalTimeType, v: LocalTime)           =>
              Value.localTime(v)
            case (StandardType.LocalDateTimeType, v: LocalDateTime)   =>
              Value.localDateTime(v)
            case (StandardType.OffsetTimeType, v: OffsetTime)         =>
              Value.offsetTime(v)
            case (StandardType.OffsetDateTimeType, v: OffsetDateTime) =>
              Value.offsetDateTime(v)
            case (StandardType.ZonedDateTimeType, v: ZonedDateTime)   =>
              Value.zonedDateTime(v)
            case (other, _)                                           =>
              throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
          }
      }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueEncoder[A],
      summoned: => Option[ValueEncoder[Option[A]]]
    ): ValueEncoder[Option[A]] =
      new ValueEncoder[Option[A]] {
        override def encode(value: Option[A]): Value =
          value match {
            case Some(v) => inner.encode(v)
            case _       => Value.nil
          }
      }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, ?],
      inner: => ValueEncoder[A],
      summoned: => Option[ValueEncoder[C[A]]]
    ): ValueEncoder[C[A]] = new ValueEncoder[C[A]] {
      override def encode(value: C[A]): Value =
        Value.list(sequence.toChunk(value).map(inner.encode))
    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => ValueEncoder[K],
      value: => ValueEncoder[V],
      summoned: => Option[ValueEncoder[Map[K, V]]]
    ): ValueEncoder[Map[K, V]] = new ValueEncoder[Map[K, V]] {
      override def encode(value0: Map[K, V]): Value =
        Value.map(
          value0.map { case (k, v) =>
            key.encode(k) -> value.encode(v)
          }
        )
    }

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[ValueEncoder, ?]],
      summoned: => Option[ValueEncoder[B]]
    ): ValueEncoder[B] = ???

  }.cached

  val summoned: Deriver[ValueEncoder] = default.autoAcceptSummoned

}
