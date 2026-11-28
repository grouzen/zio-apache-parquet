package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.{
  ParquetIterable,
  ParquetReader as P4sReader,
  ParquetRecordDecoder,
  ParquetRecordEncoder,
  ParquetSchemaResolver,
  ParquetWriter as P4sWriter,
  Path as P4sPath
}
import me.mnedokushev.zio.apache.parquet.core.codec.*
import me.mnedokushev.zio.apache.parquet.hadoop.{ ParquetReader, ParquetWriter, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.openjdk.jmh.annotations.*
import zio.{ Chunk, Runtime, Tag, Unsafe, ZIO }
import zio.schema.{ Derive, DeriveSchema, Schema }
import zio.stream.ZStream

import java.nio.file.{ Files, Path as NioPath }
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.{ Random, Using }

object ParquetBenchmarkSupport {

  case class Embedded(fraction: Double, text: String)
  case class Record(i: Int, dict: String, embedded: Option[Embedded])

  object Record {
    given Schema[Record]        = DeriveSchema.gen[Record]
    given SchemaEncoder[Record] = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.default)
    given ValueEncoder[Record]  = Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.default)
    given ValueDecoder[Record]  = Derive.derive[ValueDecoder, Record](ValueDecoderDeriver.default)
  }

  private val pageSize     = 32 * 1024
  private val rowGroupSize = 64L * 1024

  def localConfiguration(): Configuration = {
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "file:///")
    configuration
  }

  def narrowRecords(rows: Int): Vector[Record] = {
    val random = new Random(42L)
    Vector.tabulate(rows) { index =>
      val id       = index + 1
      val dict     = ('a' + random.nextInt(3)).toChar.toString
      val embedded = Option.when(id % 2 == 0) {
        Embedded(1.0 / 100.12, new UUID(random.nextLong(), random.nextLong()).toString)
      }
      Record(id, dict, embedded)
    }
  }

  def writerOptions(configuration: Configuration): P4sWriter.Options =
    P4sWriter.Options(
      writeMode = ParquetFileWriter.Mode.CREATE,
      compressionCodecName = UNCOMPRESSED,
      dictionaryEncodingEnabled = true,
      pageSize = pageSize,
      rowGroupSize = rowGroupSize,
      hadoopConf = configuration
    )

  def configuredReader[A <: Product: ValueDecoder: Tag](configuration: Configuration): ParquetReader[A] =
    run(ZIO.scoped(ParquetReader.configured[A](configuration).build.map(_.get)))

  def configuredWriter[A <: Product: Schema: SchemaEncoder: ValueEncoder: Tag](
    options: P4sWriter.Options
  ): ParquetWriter[A] =
    run(
      ZIO.scoped(
        ParquetWriter
          .configured[A](
            writeMode = options.writeMode,
            compressionCodecName = options.compressionCodecName,
            dictionaryEncodingEnabled = options.dictionaryEncodingEnabled,
            dictionaryPageSize = options.dictionaryPageSize,
            maxPaddingSize = options.maxPaddingSize,
            pageSize = options.pageSize,
            rowGroupSize = options.rowGroupSize,
            validationEnabled = options.validationEnabled,
            hadoopConf = options.hadoopConf
          )
          .build
          .map(_.get)
      )
    )

  def collectParquet4s[A](rows: ParquetIterable[A]): Chunk[A] =
    Using.resource(rows) { rows =>
      val builder = Chunk.newBuilder[A]
      rows.iterator.foreach(row => builder += row)
      builder.result()
    }

  def run[A](effect: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(effect).getOrThrowFiberFailure()
    }

  private def deleteDirectory(directory: NioPath): Unit =
    Using.resource(Files.walk(directory)) { paths =>
      paths.sorted(java.util.Comparator.reverseOrder[NioPath]()).forEach(p => Files.delete(p))
    }

  @State(Scope.Thread)
  abstract class TrialInput {
    @Param(Array("10000", "524288"))
    var rows: Int = 0

    private var directory: NioPath = scala.compiletime.uninitialized
    var input: NioPath             = scala.compiletime.uninitialized

    protected def createInput(): Unit = {
      directory = Files.createTempDirectory("parquet-read-")
      input = directory.resolve("input.parquet")
    }

    @TearDown(Level.Trial)
    def cleanup(): Unit =
      if (directory != null) deleteDirectory(directory)
  }

  private[benchmarks] val recordShapes = List("narrow", "wideFlat", "wideNested")

  @State(Scope.Thread)
  abstract class ShapedInput extends TrialInput {
    @Param(Array("narrow", "wideFlat", "wideNested"))
    var shape: String = scala.compiletime.uninitialized
  }

  @State(Scope.Thread)
  class Dataset extends ShapedInput {
    private[benchmarks] var file: StreamFile[?] = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(): Unit = {
      createInput()
      file = shape match {
        case "narrow"     => new StreamFile(input, narrowRecords(rows))
        case "wideFlat"   => new StreamFile(input, ParquetWideRecords.flatRecords(rows))
        case "wideNested" => new StreamFile(input, ParquetWideRecords.nestedRecords(rows))
        case unknown      => throw new IllegalArgumentException(s"Unknown record shape: ${unknown}")
      }
    }
  }

  private[benchmarks] class StreamFile[A <: Product: {Schema, SchemaEncoder, ValueEncoder, ValueDecoder, Tag}](
    val input: NioPath,
    val records: IndexedSeq[A]
  )(using
    val parquet4sDecoder: ParquetRecordDecoder[A],
    val parquet4sEncoder: ParquetRecordEncoder[A],
    val parquet4sSchema: ParquetSchemaResolver[A]
  ) {
    private val configuration  = localConfiguration()
    val parquet4sOptions       = writerOptions(configuration)
    val parquet4sReaderOptions = P4sReader.Options(hadoopConf = configuration)
    private val zioReader      = configuredReader[A](configuration)
    private val zioWriter      = configuredWriter[A](parquet4sOptions)
    private val chunk          = Chunk.fromIterable(records)

    P4sWriter.of[A].options(parquet4sOptions).writeAndClose(P4sPath(input), records)

    def zioRead: ZStream[zio.Scope, Throwable, A] =
      zioReader.readStream(Path(input))

    def zioWrite(output: Output): Unit =
      run(zioWriter.writeStream(Path(output.file), ZStream.fromChunk(chunk)))

    def verify(read: => Seq[A], write: Output => Unit): Unit = {
      require(read == records, "Streaming read returned different records")
      val output = new Output
      output.setup()
      try {
        write(output)
        Using.resource(P4sReader.as[A].options(parquet4sReaderOptions).read(P4sPath(output.file))) { rows =>
          require(rows.iterator.sameElements(records.iterator), "Streaming write produced different records")
        }
      } finally output.cleanup()
    }
  }

  @State(Scope.Thread)
  class Output {
    private var directory: NioPath = scala.compiletime.uninitialized
    var file: NioPath              = scala.compiletime.uninitialized

    @Setup(Level.Invocation)
    def setup(): Unit = {
      directory = Files.createTempDirectory("parquet-write-")
      file = directory.resolve("output.parquet")
    }

    @TearDown(Level.Invocation)
    def cleanup(): Unit = deleteDirectory(directory)
  }

  private[benchmarks] def verifyStreaming(
    read: [A <: Product] => StreamFile[A] => Seq[A],
    write: [A <: Product] => (StreamFile[A], Output) => Unit
  ): Unit = {
    def verify[A <: Product](file: StreamFile[A]): Unit = {
      file.verify(run(ZIO.scoped(file.zioRead.runCollect)), file.zioWrite)
      file.verify(read(file), output => write(file, output))
    }

    recordShapes.foreach { shape =>
      val data = new Dataset
      data.rows = 256
      data.shape = shape
      try {
        data.setup()
        verify(data.file)
        println(s"${shape}: verified ZIO and parquet4s streaming reads and writes")
      } finally data.cleanup()
    }
  }

}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(2)
@Threads(1)
abstract class ParquetBenchmark

abstract class ParquetStreamingBenchmark extends ParquetBenchmark {
  import ParquetBenchmarkSupport.*

  @Benchmark
  def zioStreamingRead(data: Dataset): Product =
    run(ZIO.scoped(data.file.zioRead.runLast)).get

  @Benchmark
  def zioStreamingWrite(data: Dataset, output: Output): Unit =
    data.file.zioWrite(output)

  @Benchmark
  def zioStreamingFilteredRead(data: ParquetFilterBenchmark.FilterDataset): ParquetWideRecords.WideFlat =
    run(ZIO.scoped(data.zioStream.runLast)).get
}
