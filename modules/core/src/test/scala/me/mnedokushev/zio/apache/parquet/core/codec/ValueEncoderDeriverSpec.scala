package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._
import zio.schema._
import zio.test._

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
              "a" -> Value.int(3),
              "b" -> Value.boolean(false),
              "c" -> Value.string("data"),
              "d" -> Value.nil
            )
          )
        )
      }
    )

}
