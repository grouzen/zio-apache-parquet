package me.mnedokushev.zio.apache.parquet.core.codec

//import me.mnedokushev.zio.apache.parquet.core.Value
//import me.mnedokushev.zio.apache.parquet.core.Value.PrimitiveValue
import zio._
import zio.schema._
import zio.test._

import java.util.UUID

//import java.nio.ByteBuffer
//import java.util.UUID
//import scala.annotation.nowarn

object ValueCodecDeriverSpec extends ZIOSpecDefault {

  case class Record(a: Int, b: Boolean, c: Option[String], d: List[Int], e: Map[String, Int])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  case class SummonedRecord(a: Int, b: Boolean, c: Option[String], d: Option[Int])
  object SummonedRecord {
    implicit val schema: Schema[SummonedRecord] = DeriveSchema.gen[SummonedRecord]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ValueCodecDeriverSpec")(
      test("primitive") {
        val stringEncoder  = Derive.derive[ValueEncoder, String](ValueEncoderDeriver.default)
        val booleanEncoder = Derive.derive[ValueEncoder, Boolean](ValueEncoderDeriver.default)
        val byteEncoder    = Derive.derive[ValueEncoder, Byte](ValueEncoderDeriver.default)
        val shortEncoder   = Derive.derive[ValueEncoder, Short](ValueEncoderDeriver.default)
        val intEncoder     = Derive.derive[ValueEncoder, Int](ValueEncoderDeriver.default)
        val longEncoder    = Derive.derive[ValueEncoder, Long](ValueEncoderDeriver.default)
        val uuidEncoder    = Derive.derive[ValueEncoder, UUID](ValueEncoderDeriver.default)

        val stringDecoder  = Derive.derive[ValueDecoder, String](ValueDecoderDeriver.default)
        val booleanDecoder = Derive.derive[ValueDecoder, Boolean](ValueDecoderDeriver.default)
        val byteDecoder    = Derive.derive[ValueDecoder, Byte](ValueDecoderDeriver.default)
        val shortDecoder   = Derive.derive[ValueDecoder, Short](ValueDecoderDeriver.default)
        val intDecoder     = Derive.derive[ValueDecoder, Int](ValueDecoderDeriver.default)
        val longDecoder    = Derive.derive[ValueDecoder, Long](ValueDecoderDeriver.default)
        val uuidDecoder    = Derive.derive[ValueDecoder, UUID](ValueDecoderDeriver.default)

        val stringPayload  = "foo"
        val booleanPayload = false
        val bytePayload    = 10.toByte
        val shortPayload   = 30.toShort
        val intPayload     = 254
        val longPayload    = 398812L
        val uuidPayload    = UUID.randomUUID()

        for {
          stringValue   <- stringEncoder.encodeZIO(stringPayload)
          stringResult  <- stringDecoder.decodeZIO(stringValue)
          booleanValue  <- booleanEncoder.encodeZIO(booleanPayload)
          booleanResult <- booleanDecoder.decodeZIO(booleanValue)
          byteValue     <- byteEncoder.encodeZIO(bytePayload)
          byteResult    <- byteDecoder.decodeZIO(byteValue)
          shortValue    <- shortEncoder.encodeZIO(shortPayload)
          shortResult   <- shortDecoder.decodeZIO(shortValue)
          intValue      <- intEncoder.encodeZIO(intPayload)
          intResult     <- intDecoder.decodeZIO(intValue)
          longValue     <- longEncoder.encodeZIO(longPayload)
          longResult    <- longDecoder.decodeZIO(longValue)
          uuidValue     <- uuidEncoder.encodeZIO(uuidPayload)
          uuidResult    <- uuidDecoder.decodeZIO(uuidValue)
        } yield assertTrue(
          stringResult == stringPayload,
          booleanResult == booleanPayload,
          byteResult == bytePayload,
          shortResult == shortPayload,
          intResult == intPayload,
          longResult == longPayload,
          uuidResult == uuidPayload
        )
      },
      test("option") {
        val encoder  = Derive.derive[ValueEncoder, Option[Int]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, Option[Int]](ValueDecoderDeriver.default)
        val payloads = List(Option(3), Option.empty)

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("sequence") {
        val encoder  = Derive.derive[ValueEncoder, List[Map[String, Int]]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, List[Map[String, Int]]](ValueDecoderDeriver.default)
        val payloads = List(List(Map("foo" -> 1, "bar" -> 2)), List.empty)

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("map") {
        val encoder  = Derive.derive[ValueEncoder, Map[String, Int]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, Map[String, Int]](ValueDecoderDeriver.default)
        val payloads = List(Map("foo" -> 3), Map.empty[String, Int])

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("record") {
        val encoder = Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.default)
        val decoder = Derive.derive[ValueDecoder, Record](ValueDecoderDeriver.default)
        val payload = Record(2, false, Some("data"), List(1), Map("zio" -> 1))

        for {
          value  <- encoder.encodeZIO(payload)
          result <- decoder.decodeZIO(value)
        } yield assertTrue(result == payload)
      }
//      test("summoned") {
      //        @nowarn
      //        implicit val stringEncoder: ValueEncoder[String] = new ValueEncoder[String] {
      //          override def encode(value: String): Value =
      //            Value.uuid(UUID.fromString(value))
      //        }
      //        @nowarn
      //        implicit val stringDecoder: ValueDecoder[String] = new ValueDecoder[String] {
      //          override def decode(value: Value): String =
      //            value match {
      //              case PrimitiveValue.ByteArrayValue(v) if v.length == 16 =>
      //                val bb = ByteBuffer.wrap(v.getBytes)
      //                new UUID(bb.getLong, bb.getLong).toString
      //              case other                                              =>
      //                throw DecoderError(s"Wrong value: $other")
      //
      //            }
      //        }
      //
      //        val uuid    = UUID.randomUUID()
      //        val encoder = Derive.derive[ValueEncoder, SummonedRecord](ValueEncoderDeriver.summoned)
      //        val decoder = Derive.derive[ValueDecoder, SummonedRecord](ValueDecoderDeriver.summoned)
      //        val payload = SummonedRecord(2, false, Some(uuid.toString), None)
      //
      //        for {
      //          value  <- encoder.encodeZIO(payload)
      //          result <- decoder.decodeZIO(value)
      //        } yield assertTrue(result == payload)
      //      }
    )

}
