package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Schemas.PrimitiveDef
import org.apache.parquet.schema.Type
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object SchemaEncoderDeriver {

  val default: Deriver[SchemaEncoder] = new Deriver[SchemaEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, ?]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {

      private def enc[A1](name0: String, schema0: Schema[A1], encoder: SchemaEncoder[?]) =
        encoder.asInstanceOf[SchemaEncoder[A1]].encode(schema0, name0, isSchemaOptional(schema0))

      override def encode(schema: Schema[A], name: String, optional: Boolean): Type = {
        val fieldTypes = record.fields.zip(fields.map(_.unwrap)).map { case (field, encoder) =>
          enc(field.name, field.schema, encoder)
        }

        Schemas.record(fieldTypes).optionality(optional).named(name)
      }
    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[SchemaEncoder, ?]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {
      override def encode(schema: Schema[A], name: String, optional: Boolean): Type =
        Schemas.enum0.optionality(optional).named(name)
    }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] =
      new SchemaEncoder[A] {
        override def encode(schema: Schema[A], name: String, optional: Boolean): Type = {
          def tpe(prim: PrimitiveDef) =
            prim.optionality(optional).named(name)

          st match {
            case StandardType.StringType         =>
              tpe(Schemas.string)
            case StandardType.BoolType           =>
              tpe(Schemas.boolean)
            case StandardType.ByteType           =>
              tpe(Schemas.byte)
            case StandardType.ShortType          =>
              tpe(Schemas.short)
            case StandardType.IntType            =>
              tpe(Schemas.int)
            case StandardType.LongType           =>
              tpe(Schemas.long)
            case StandardType.FloatType          =>
              tpe(Schemas.float)
            case StandardType.DoubleType         =>
              tpe(Schemas.double)
            case StandardType.BinaryType         =>
              tpe(Schemas.binary)
            case StandardType.CharType           =>
              tpe(Schemas.char)
            case StandardType.UUIDType           =>
              tpe(Schemas.uuid)
            case StandardType.CurrencyType       =>
              tpe(Schemas.currency)
            case StandardType.BigDecimalType     =>
              tpe(Schemas.bigDecimal)
            case StandardType.BigIntegerType     =>
              tpe(Schemas.bigInteger)
            case StandardType.DayOfWeekType      =>
              tpe(Schemas.dayOfWeek)
            case StandardType.MonthType          =>
              tpe(Schemas.monthType)
            case StandardType.MonthDayType       =>
              tpe(Schemas.monthDay)
            case StandardType.PeriodType         =>
              tpe(Schemas.period)
            case StandardType.YearType           =>
              tpe(Schemas.year)
            case StandardType.YearMonthType      =>
              tpe(Schemas.yearMonth)
            case StandardType.ZoneIdType         =>
              tpe(Schemas.zoneId)
            case StandardType.ZoneOffsetType     =>
              tpe(Schemas.zoneOffset)
            case StandardType.DurationType       =>
              tpe(Schemas.duration)
            case StandardType.InstantType        =>
              tpe(Schemas.instant)
            case StandardType.LocalDateType      =>
              tpe(Schemas.localDate)
            case StandardType.LocalTimeType      =>
              tpe(Schemas.localTime)
            case StandardType.LocalDateTimeType  =>
              tpe(Schemas.localDateTime)
            case StandardType.OffsetTimeType     =>
              tpe(Schemas.offsetTime)
            case StandardType.OffsetDateTimeType =>
              tpe(Schemas.offsetDateTime)
            case StandardType.ZonedDateTimeType  =>
              tpe(Schemas.zonedDateTime)
            case StandardType.UnitType           =>
              throw EncoderError("Unit standard type is unsupported")
          }
        }
      }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => SchemaEncoder[A],
      summoned: => Option[SchemaEncoder[Option[A]]]
    ): SchemaEncoder[Option[A]] = new SchemaEncoder[Option[A]] {
      override def encode(schema: Schema[Option[A]], name: String, optional: Boolean): Type =
        inner.encode(option.schema, name, optional = true)
    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, ?],
      inner: => SchemaEncoder[A],
      summoned: => Option[SchemaEncoder[C[A]]]
    ): SchemaEncoder[C[A]] = new SchemaEncoder[C[A]] {
      override def encode(schema: Schema[C[A]], name: String, optional: Boolean): Type =
        Schemas
          .list(inner.encode(sequence.elementSchema, "element", isSchemaOptional(sequence.elementSchema)))
          .optionality(optional)
          .named(name)
    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => SchemaEncoder[K],
      value: => SchemaEncoder[V],
      summoned: => Option[SchemaEncoder[Map[K, V]]]
    ): SchemaEncoder[Map[K, V]] = new SchemaEncoder[Map[K, V]] {
      override def encode(schema: Schema[Map[K, V]], name: String, optional: Boolean): Type =
        Schemas
          .map(
            key.encode(map.keySchema, "key", optional = false),
            value.encode(map.valueSchema, "value", optional = isSchemaOptional(map.valueSchema))
          )
          .optionality(optional)
          .named(name)
    }

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, ?]],
      summoned: => Option[SchemaEncoder[B]]
    ): SchemaEncoder[B] = ???

  }.cached

  val summoned: Deriver[SchemaEncoder] = default.autoAcceptSummoned

  private def isSchemaOptional(schema: Schema[?]): Boolean =
    schema match {
      case _: Schema.Optional[_] => true
      case _                     => false
    }
}
