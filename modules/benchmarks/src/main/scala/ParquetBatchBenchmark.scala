package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.{
  ParquetReader as P4sReader,
  ParquetRecordDecoder,
  ParquetRecordEncoder,
  ParquetSchemaResolver,
  ParquetWriter as P4sWriter,
  Path as P4sPath
}
import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.codec.{ SchemaEncoder, ValueDecoder, ValueEncoder }
import me.mnedokushev.zio.apache.parquet.hadoop.Path
import org.apache.parquet.schema.MessageType
import org.openjdk.jmh.annotations.*
import zio.{ Chunk, Tag }
import zio.schema.Schema

import java.nio.file.Path as NioPath
import scala.jdk.CollectionConverters.*
import scala.util.Using

import ParquetBenchmarkSupport.*
import ParquetWideRecords.*

object ParquetBatchBenchmark {

  private val shapes = List("narrow", "wideFlat", "wideNested")

  @State(Scope.Thread)
  class BatchDataset extends TrialInput {
    @Param(Array("narrow", "wideFlat", "wideNested"))
    var shape: String = scala.compiletime.uninitialized

    private[benchmarks] var file: BatchFile[?] = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(): Unit = {
      createInput()
      file = shape match {
        case "narrow"     => new BatchFile(input, Chunk.fromIterable(narrowRecords(rows)))
        case "wideFlat"   => new BatchFile(input, flatRecords(rows))
        case "wideNested" => new BatchFile(input, nestedRecords(rows))
        case unknown      => throw new IllegalArgumentException(s"Unknown record shape: ${unknown}")
      }
    }
  }

  private[benchmarks] class BatchFile[
    A <: Product: {Schema, SchemaEncoder, ValueEncoder, ValueDecoder, Tag, ParquetRecordEncoder, ParquetRecordDecoder,
      ParquetSchemaResolver, JavaParquetRecords.Mapping}
  ](val input: NioPath, records: Chunk[A]) {
    private val configuration         = localConfiguration()
    private val writerConfig          = writerOptions(configuration)
    private val parquet4sReaderConfig = P4sReader.Options(hadoopConf = configuration)
    private val zioReader             = configuredReader[A](configuration)
    private val zioWriter             = configuredWriter[A](writerConfig)
    private val javaWriteSchema       = Schemas.asMessageType(
      summon[SchemaEncoder[A]].encode(summon[Schema[A]], summon[Tag[A]].tag.shortName, optional = false)
    )

    parquet4sWrite(input)

    def zioRead(file: NioPath): Chunk[A] =
      run(zioReader.readChunk[A](Path(file)))

    def parquet4sRead(file: NioPath): Chunk[A] =
      Using.resource(P4sReader.as[A].options(parquet4sReaderConfig).read(P4sPath(file))) { rows =>
        val builder = Chunk.newBuilder[A]
        rows.iterator.foreach(row => builder += row)
        builder.result()
      }

    def javaRead(file: NioPath): Chunk[A] =
      JavaParquetRecords.read[A](file, configuration)

    def zioWrite(file: NioPath): Unit =
      run(zioWriter.writeChunk(Path(file), records))

    def parquet4sWrite(file: NioPath): Unit =
      P4sWriter.of[A].options(writerConfig).writeAndClose(P4sPath(file), records)

    def javaWrite(file: NioPath): Unit =
      JavaParquetRecords.write(file, records, javaWriteSchema, writerConfig)

    private def columns(schema: MessageType) =
      schema.getColumns.asScala.map { column =>
        val primitive = column.getPrimitiveType
        (column.getPath.toList, primitive.getPrimitiveTypeName, primitive.getLogicalTypeAnnotation)
      }.toList

    def verify(): Unit = {
      val readers     = List("ZIO" -> zioRead, "parquet4s" -> parquet4sRead, "Java" -> javaRead)
      val inputSchema = JavaParquetRecords.schema(input, configuration)
      val writers     = List(
        ("ZIO", zioWrite, javaWriteSchema),
        ("parquet4s", parquet4sWrite, inputSchema),
        ("Java", javaWrite, javaWriteSchema)
      )

      readers.foreach { case (name, read) =>
        require(read(input) == records, s"${name} read different records from the common input")
      }

      writers.foreach { case (writerName, write, expectedSchema) =>
        val output = new Output
        output.setup()
        try {
          write(output.file)
          val schema = JavaParquetRecords.schema(output.file, configuration)
          require(
            schema == expectedSchema && columns(schema) == columns(inputSchema),
            s"${writerName} wrote an unexpected schema:\n${schema}\nExpected:\n${expectedSchema}"
          )
          readers.foreach { case (readerName, read) =>
            require(read(output.file) == records, s"${readerName} read different records from ${writerName} output")
          }
        } finally output.cleanup()
      }
    }
  }

  def main(args: Array[String]): Unit =
    shapes.foreach { shape =>
      val dataset = new BatchDataset
      dataset.rows = 256
      dataset.shape = shape
      try {
        dataset.setup()
        dataset.file.verify()
        println(s"${shape}: verified the common input and all reader/writer pairs")
      } finally dataset.cleanup()
    }
}

class ParquetBatchBenchmark extends ParquetBenchmark {
  import ParquetBatchBenchmark.BatchDataset

  @Benchmark
  def zioBatchRead(data: BatchDataset): Chunk[?] =
    data.file.zioRead(data.input)

  @Benchmark
  def parquet4sBatchRead(data: BatchDataset): Chunk[?] =
    data.file.parquet4sRead(data.input)

  @Benchmark
  def javaBatchRead(data: BatchDataset): Chunk[?] =
    data.file.javaRead(data.input)

  @Benchmark
  def zioBatchWrite(data: BatchDataset, output: Output): Unit =
    data.file.zioWrite(output.file)

  @Benchmark
  def parquet4sBatchWrite(data: BatchDataset, output: Output): Unit =
    data.file.parquet4sWrite(output.file)

  @Benchmark
  def javaBatchWrite(data: BatchDataset, output: Output): Unit =
    data.file.javaWrite(output.file)
}
