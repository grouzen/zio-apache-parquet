package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Schemas.PrimitiveDef
import zio._
import zio.schema._
import zio.test._

import java.util.UUID

object SchemaEncoderDeriverSpec extends ZIOSpecDefault {

  case class Record(a: Int, b: Option[String])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaEncoderDeriverSpec")(
      test("primitive") {
        def encode[A](encoder: SchemaEncoder[_], schema: Schema[A], name: String, optional: Boolean) =
          encoder.asInstanceOf[SchemaEncoder[A]].encode(schema, name, optional)

        def named(defs: List[PrimitiveDef], names: List[String]) =
          defs.zip(names).map { case (schemaDef, name) =>
            schemaDef.named(name)
          }

        val encoders: List[SchemaEncoder[_]] =
          List(
            Derive.derive[SchemaEncoder, String](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Boolean](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Byte](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Short](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Int](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Long](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, UUID](SchemaEncoderDeriver.default)
          )
        val schemas: List[Schema[_]]         =
          List(
            Schema.primitive[String],
            Schema.primitive[Boolean],
            Schema.primitive[Byte],
            Schema.primitive[Short],
            Schema.primitive[Int],
            Schema.primitive[Long],
            Schema.primitive[UUID]
          )
        val names                            =
          List(
            "string",
            "boolean",
            "byte",
            "short",
            "int",
            "long",
            "uuid"
          )
        val schemaDefs                       = List(
          Schemas.string,
          Schemas.boolean,
          Schemas.byte,
          Schemas.short,
          Schemas.int,
          Schemas.long,
          Schemas.uuid
        )
        val optionalDefs                     =
          schemaDefs.map(_.optional)
        val requiredDefs                     =
          schemaDefs.map(_.required)

        val expectedOptional = named(optionalDefs, names)
        val expectedRequired = named(requiredDefs, names)

        encoders
          .zip(schemas)
          .zip(names)
          .zip(expectedOptional)
          .zip(expectedRequired)
          .map { case ((((encoder, schema), name), expOptional), expRequired) =>
            val tpeOptional = encode(encoder, schema, name, optional = true)
            val tpeRequired = encode(encoder, schema, name, optional = false)

            assertTrue(tpeOptional == expOptional, tpeRequired == expRequired)
          }
          .reduce(_ && _)
      },
      test("record") {
        val encoder     = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.default)
        val tpeOptional = encoder.encode(Record.schema, "record", optional = true)
        val tpeRequired = encoder.encode(Record.schema, "record", optional = false)
        val schemaDef   = Schemas.record(
          Chunk(
            Schemas.int.required.named("a"),
            Schemas.string.optional.named("b")
          )
        )

        assertTrue(
          tpeOptional == schemaDef.optional.named("record"),
          tpeRequired == schemaDef.required.named("record")
        )
      }
    )

}
