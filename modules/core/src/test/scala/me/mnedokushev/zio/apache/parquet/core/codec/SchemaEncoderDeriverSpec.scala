package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Schemas.PrimitiveDef
import zio._
import zio.schema._
import zio.test._

import java.util.UUID
//import scala.annotation.nowarn

object SchemaEncoderDeriverSpec extends ZIOSpecDefault {

  case class Record(a: Int, b: Option[String])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  // Helper for being able to extract type parameter A from a given schema in order to cast the type of encoder<
  private def encode[A](encoder: SchemaEncoder[_], schema: Schema[A], name: String, optional: Boolean) =
    encoder.asInstanceOf[SchemaEncoder[A]].encode(schema, name, optional)

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaEncoderDeriverSpec")(
      test("primitive") {
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
        val name        = "record"
        val encoder     = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.default)
        val tpeOptional = encoder.encode(Record.schema, name, optional = true)
        val tpeRequired = encoder.encode(Record.schema, name, optional = false)
        val schemaDef   = Schemas.record(
          Chunk(
            Schemas.int.required.named("a"),
            Schemas.string.optional.named("b")
          )
        )

        assertTrue(
          tpeOptional == schemaDef.optional.named(name),
          tpeRequired == schemaDef.required.named(name)
        )
      },
      test("sequence") {
        val name                             = "mylist"
        val encoders: List[SchemaEncoder[_]] =
          List(
            Derive.derive[SchemaEncoder, List[String]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Boolean]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Byte]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Short]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Int]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Long]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[UUID]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[String]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Boolean]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Byte]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Short]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Int]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Long]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[UUID]]](SchemaEncoderDeriver.default)
          )
        val schemas: List[Schema[_]]         =
          List(
            Schema.list[String],
            Schema.list[Int],
            Schema.list[Option[String]],
            Schema.list[Option[Int]]
          )
        val elements                         =
          List(
            Schemas.string,
            Schemas.boolean,
            Schemas.byte,
            Schemas.short,
            Schemas.int,
            Schemas.long,
            Schemas.uuid
          )
        val schemaDefs                       =
          (elements.map(_.required) ++ elements.map(_.optional))
            .map(_.named("element"))
            .map(Schemas.list)
        val expectedOptional                 =
          schemaDefs.map(_.optional.named(name))
        val expectedRequired                 =
          schemaDefs.map(_.required.named(name))

        encoders
          .zip(schemas)
          .zip(expectedOptional)
          .zip(expectedRequired)
          .map { case (((encoder, schema), expOptional), expRequired) =>
            val tpeOptional = encode(encoder, schema, name, optional = true)
            val tpeRequired = encode(encoder, schema, name, optional = false)

            assertTrue(
              tpeOptional == expOptional,
              tpeRequired == expRequired
            )
          }
          .reduce(_ && _)
      },
      test("map") {
        val name = "mymap"

        val encoder = Derive.derive[SchemaEncoder, Map[String, Int]](SchemaEncoderDeriver.default)
        val tpe     = encoder.encode(Schema.map[String, Int], name, optional = true)

        assertTrue(
          tpe == Schemas
            .map(Schemas.string.required.named("key"), Schemas.int.required.named("value"))
            .optional
            .named(name)
        )
      }
//      test("summoned") {
      //        // @nowarn annotation is needed to avoid having 'variable is not used' compiler error
      //        @nowarn
      //        implicit val intEncoder: SchemaEncoder[Int] = new SchemaEncoder[Int] {
      //          override def encode(schema: Schema[Int], name: String, optional: Boolean): Type =
      //            Schemas.uuid.optionality(optional).named(name)
      //        }
      //
      //        val name    = "myrecord"
      //        val encoder = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.summoned)
      //        val tpe     = encoder.encode(Record.schema, name, optional = true)
      //
      //        assertTrue(
      //          tpe == Schemas
      //            .record(Chunk(Schemas.uuid.required.named("a"), Schemas.string.optional.named("b")))
      //            .optional
      //            .named(name)
      //        )
      //      }
    )

}
