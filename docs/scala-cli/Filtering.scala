//> using scala "3.7.1"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.3.1

import zio.*
import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*
import me.mnedokushev.zio.apache.parquet.hadoop.{ ParquetReader, ParquetWriter, Path }
import me.mnedokushev.zio.apache.parquet.core.filter.syntax.*
import me.mnedokushev.zio.apache.parquet.core.filter.*

import java.nio.file.Files

object Filtering extends ZIOAppDefault:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    // We need to provide field names using singleton types
    given Schema.CaseClass3.WithFields["a", "b", "c", Int, String, Option[Long], MyRecord] =
      DeriveSchema.gen[MyRecord]
    given SchemaEncoder[MyRecord]                                                          =
      Derive.derive[SchemaEncoder, MyRecord](SchemaEncoderDeriver.default)
    given ValueEncoder[MyRecord]                                                           =
      Derive.derive[ValueEncoder, MyRecord](ValueEncoderDeriver.default)
    given ValueDecoder[MyRecord]                                                           =
      Derive.derive[ValueDecoder, MyRecord](ValueDecoderDeriver.default)
    given TypeTag[MyRecord]                                                                =
      Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    // Define accessors to use them later in the filter predicate.
    // You can give any names to the accessors as we demonstrate here.
    val (id, name, age) = Filter[MyRecord].columns

  val data =
    Chunk(
      MyRecord(1, "bob", Some(10L)),
      MyRecord(2, "bob", Some(12L)),
      MyRecord(3, "alice", Some(13L)),
      MyRecord(4, "john", None)
    )

  val recordsFile = Path(Files.createTempDirectory("records")) / "records.parquet"

  override def run =
    (
      for {
        writer   <- ZIO.service[ParquetWriter[MyRecord]]
        reader   <- ZIO.service[ParquetReader[MyRecord]]
        _        <- writer.writeChunk(recordsFile, data)
        fromFile <- reader.readChunkFiltered(
                      recordsFile,
                      filter(
                        MyRecord.id > 1 `and` (
                          MyRecord.name =!= "bob" `or`
                            // Use .nullable syntax for optional fields.
                            MyRecord.age.nullable > 10L
                        )
                      )
                    )
        _        <- Console.printLine(fromFile)
      } yield ()
    ).provide(
      ParquetWriter.configured[MyRecord](),
      ParquetReader.configured[MyRecord]()
    )
  // Outputs:
  // Chunk(MyRecord(2,bob,Some(12)),MyRecord(3,alice,Some(13)),MyRecord(4,john,None))
