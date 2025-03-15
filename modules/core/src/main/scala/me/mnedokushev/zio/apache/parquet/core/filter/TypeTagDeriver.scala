package me.mnedokushev.zio.apache.parquet.core.filter

import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object TypeTagDeriver {

  val default: Deriver[TypeTag] = new Deriver[TypeTag] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[TypeTag, ?]],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] =
      TypeTag.Record(
        record.fields
          .map(_.name.toString)
          .zip(fields.map(_.unwrap))
          .toMap
      )

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[TypeTag, ?]],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] = {
      val casesMap = `enum`.cases.map { case0 =>
        case0.schema.asInstanceOf[Schema.CaseClass0[A]].defaultConstruct() -> case0.id
      }.toMap

      TypeTag.enum0(casesMap)
    }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[TypeTag[A]]
    ): TypeTag[A] =
      st match {
        case StandardType.StringType         => TypeTag.string
        case StandardType.BoolType           => TypeTag.boolean
        case StandardType.ByteType           => TypeTag.byte
        case StandardType.ShortType          => TypeTag.short
        case StandardType.IntType            => TypeTag.int
        case StandardType.LongType           => TypeTag.long
        case StandardType.FloatType          => TypeTag.float
        case StandardType.DoubleType         => TypeTag.double
        case StandardType.BinaryType         => TypeTag.binary
        case StandardType.CharType           => TypeTag.char
        case StandardType.UUIDType           => TypeTag.uuid
        case StandardType.CurrencyType       => TypeTag.currency
        case StandardType.BigDecimalType     => TypeTag.bigDecimal
        case StandardType.BigIntegerType     => TypeTag.bigInteger
        case StandardType.DayOfWeekType      => TypeTag.dayOfWeek
        case StandardType.MonthType          => TypeTag.month
        case StandardType.MonthDayType       => TypeTag.monthDay
        case StandardType.PeriodType         => TypeTag.period
        case StandardType.YearType           => TypeTag.year
        case StandardType.YearMonthType      => TypeTag.yearMonth
        case StandardType.ZoneIdType         => TypeTag.zoneId
        case StandardType.ZoneOffsetType     => TypeTag.zoneOffset
        case StandardType.DurationType       => TypeTag.duration
        case StandardType.InstantType        => TypeTag.instant
        case StandardType.LocalDateType      => TypeTag.localDate
        case StandardType.LocalTimeType      => TypeTag.localTime
        case StandardType.LocalDateTimeType  => TypeTag.localDateTime
        case StandardType.OffsetTimeType     => TypeTag.offsetTime
        case StandardType.OffsetDateTimeType => TypeTag.offsetDateTime
        case StandardType.ZonedDateTimeType  => TypeTag.zonedDateTime
        case StandardType.UnitType           => TypeTag.dummy[A]
      }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => TypeTag[A],
      summoned: => Option[TypeTag[Option[A]]]
    ): TypeTag[Option[A]] =
      TypeTag.optional[A](using inner)

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, ?],
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
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[TypeTag, ?]],
      summoned: => Option[TypeTag[B]]
    ): TypeTag[B] =
      TypeTag.dummy[B]

  }.cached

  val summoned: Deriver[TypeTag] = default.autoAcceptSummoned

}
