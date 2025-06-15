//> using scala "3.6.4"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.2.2

import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*

object Value extends App:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    given Schema[MyRecord]                =
      DeriveSchema.gen[MyRecord]
    given encoder: ValueEncoder[MyRecord] =
      Derive.derive[ValueEncoder, MyRecord](ValueEncoderDeriver.default)
    given decoder: ValueDecoder[MyRecord] =
      Derive.derive[ValueDecoder, MyRecord](ValueDecoderDeriver.default)

  val value  = MyRecord.encoder.encode(MyRecord(3, "zio", None))
  val record = MyRecord.decoder.decode(value)

  println(value)
  // Outputs:
  // RecordValue(Map(a -> Int32Value(3), b -> BinaryValue(Binary{"zio"}), c -> NullValue))
  println(record)
  // Outputs:
  // MyRecord(3,zio,None)
