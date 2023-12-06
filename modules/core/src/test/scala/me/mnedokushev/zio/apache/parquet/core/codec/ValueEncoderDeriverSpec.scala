package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._
import zio.schema._
import zio.test._

import java.util.UUID
import scala.annotation.nowarn

object ValueEncoderDeriverSpec extends ZIOSpecDefault {

  case class Record(a: Int, b: Boolean, c: Option[String], d: Option[Int])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ValueEncoderDeriverSpec")(
      test("record") {
        val encoder = Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.default)
        val value   = encoder.encode(Record(2, false, Some("data"), None))

        assertTrue(
          value == Value.record(
            Map(
              "a" -> Value.int(2),
              "b" -> Value.boolean(false),
              "c" -> Value.string("data"),
              "d" -> Value.nil
            )
          )
        )
      },
      test("summoned") {
        @nowarn
        implicit val stringEncoder: ValueEncoder[String] = new ValueEncoder[String] {
          override def encode(value: String): Value =
            Value.uuid(UUID.fromString(value))
        }

        val uuid    = UUID.randomUUID()
        val encoder = Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.summoned)
        val value   = encoder.encode(Record(2, false, Some(uuid.toString), None))

        assertTrue(
          value == Value.record(
            Map(
              "a" -> Value.int(2),
              "b" -> Value.boolean(false),
              "c" -> Value.uuid(uuid),
              "d" -> Value.nil
            )
          )
        )
      }
    )

}
