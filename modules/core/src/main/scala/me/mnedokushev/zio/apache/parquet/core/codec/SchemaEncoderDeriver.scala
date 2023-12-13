package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Schemas
import org.apache.parquet.schema.Type
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object SchemaEncoderDeriver {

  val default: Deriver[SchemaEncoder] = new Deriver[SchemaEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {

      private def enc[A1](name0: String, schema0: Schema[A1], encoder: SchemaEncoder[_]) =
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
      cases: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {
      override def encode(schema: Schema[A], name: String, optional: Boolean): Type =
        Schemas.string.optionality(optional).named(name)
    }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] =
      new SchemaEncoder[A] {
        override def encode(schema: Schema[A], name: String, optional: Boolean): Type =
          st match {
            case StandardType.StringType =>
              Schemas.string.optionality(optional).named(name)
            case StandardType.BoolType   =>
              Schemas.boolean.optionality(optional).named(name)
            case StandardType.ByteType   =>
              Schemas.byte.optionality(optional).named(name)
            case StandardType.ShortType  =>
              Schemas.short.optionality(optional).named(name)
            case StandardType.IntType    =>
              Schemas.int.optionality(optional).named(name)
            case StandardType.LongType   =>
              Schemas.long.optionality(optional).named(name)
            // TODO: add the other types
            case StandardType.UUIDType   =>
              Schemas.uuid.optionality(optional).named(name)
            case _                       => ???
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
      sequence: Schema.Sequence[C[A], A, _],
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
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[B]]
    ): SchemaEncoder[B] = ???

  }.cached

  val summoned: Deriver[SchemaEncoder] = default.autoAcceptSummoned

  private def isSchemaOptional(schema: Schema[_]): Boolean =
    schema match {
      case _: Schema.Optional[_] => true
      case _                     => false
    }
}
