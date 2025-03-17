//> using scala "3.5.0"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.1.10

import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*
import me.mnedokushev.zio.apache.parquet.core.hadoop.{ ParquetReader, ParquetWriter, Path }
import zio.*

import java.nio.file.Files

object ParquetIO extends ZIOAppDefault:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    given Schema[MyRecord]        =
      DeriveSchema.gen[MyRecord]
    given SchemaEncoder[MyRecord] =
      Derive.derive[SchemaEncoder, MyRecord](SchemaEncoderDeriver.default)
    given ValueEncoder[MyRecord]  =
      Derive.derive[ValueEncoder, MyRecord](ValueEncoderDeriver.default)
    given ValueDecoder[MyRecord]  =
      Derive.derive[ValueDecoder, MyRecord](ValueDecoderDeriver.default)

  val data =
    Chunk(
      MyRecord(1, "first", Some(11)),
      MyRecord(3, "third", None)
    )

  val recordsFile = Path(Files.createTempDirectory("records")) / "records.parquet"

  override def run =
    (for {
      writer   <- ZIO.service[ParquetWriter[MyRecord]]
      reader   <- ZIO.service[ParquetReader[MyRecord]]
      _        <- writer.writeChunk(recordsFile, data)
      fromFile <- reader.readChunk(recordsFile)
      _        <- Console.printLine(fromFile)
    } yield ()).provide(
      ParquetWriter.configured[MyRecord](),
      ParquetReader.configured[MyRecord]()
    )
  // Outputs:
  // Chunk(MyRecord(1,first,Some(11)),MyRecord(3,third,None))
