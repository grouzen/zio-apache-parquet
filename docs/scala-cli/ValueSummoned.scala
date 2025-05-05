//> using scala "3.5.0"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.1.12

import me.mnedokushev.zio.apache.parquet.core.Value
import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*

import java.nio.charset.StandardCharsets

object ValueSummoned extends App:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    given Schema[MyRecord] =
      DeriveSchema.gen[MyRecord]
    given ValueEncoder[Int] with {
      override def encode(value: Int): Value =
        Value.string(value.toString)
    }
    given ValueDecoder[Int] with {
      override def decode(value: Value): Int =
        value match {
          case Value.PrimitiveValue.BinaryValue(v) =>
            new String(v.getBytes, StandardCharsets.UTF_8).toInt
          case other                               =>
            throw DecoderError(s"Wrong value: $other")
        }
    }
    given encoder: ValueEncoder[MyRecord] =
      Derive.derive[ValueEncoder, MyRecord](ValueEncoderDeriver.summoned)
    given decoder: ValueDecoder[MyRecord] =
      Derive.derive[ValueDecoder, MyRecord](ValueDecoderDeriver.summoned)

  val value  = MyRecord.encoder.encode(MyRecord(3, "zio", None))
  val record = MyRecord.decoder.decode(value)

  println(value)
  // Outputs:
  // RecordValue(Map(a -> BinaryValue(Binary{"3"}), b -> BinaryValue(Binary{"zio"}), c -> NullValue))
  println(record)
  // Outputs:
  // MyRecord(3,zio,None)
