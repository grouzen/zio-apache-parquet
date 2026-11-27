package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.{ ParquetReader as P4sReader, ParquetWriter as P4sWriter, Path as P4sPath }
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

  @State(Scope.Thread)
  class Dataset extends TrialInput {
    var records: Vector[Record]                   = scala.compiletime.uninitialized
    var chunk: Chunk[Record]                      = scala.compiletime.uninitialized
    var zioReader: ParquetReader[Record]          = scala.compiletime.uninitialized
    var zioWriter: ParquetWriter[Record]          = scala.compiletime.uninitialized
    var parquet4sOptions: P4sWriter.Options       = scala.compiletime.uninitialized
    var parquet4sReaderOptions: P4sReader.Options = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(): Unit = {
      records = narrowRecords(rows)
      chunk = Chunk.fromIterable(records)
      createInput()
      val configuration = localConfiguration()
      parquet4sOptions = writerOptions(configuration)
      parquet4sReaderOptions = P4sReader.Options(hadoopConf = configuration)
      zioReader = configuredReader[Record](configuration)
      zioWriter = configuredWriter[Record](parquet4sOptions)
      P4sWriter.of[Record].options(parquet4sOptions).writeAndClose(P4sPath(input), records)
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

  def verify(
    read: Dataset => Seq[Record],
    write: (Dataset, Output) => Unit
  ): Unit = {
    val data = new Dataset
    data.rows = 64
    data.setup()
    try {
      require(read(data) == data.records, "Streaming read returned different records")
      val output = new Output
      output.setup()
      try {
        write(data, output)
        Using.resource(P4sReader.as[Record].options(data.parquet4sReaderOptions).read(P4sPath(output.file))) { rows =>
          require(rows.iterator.toVector == data.records, "Streaming write produced different records")
        }
      } finally output.cleanup()
    } finally data.cleanup()
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
  def zioStreamingRead(data: Dataset): Record =
    run(ZIO.scoped(data.zioReader.readStream(Path(data.input)).runLast)).get

  @Benchmark
  def zioStreamingWrite(data: Dataset, output: Output): Unit =
    run(data.zioWriter.writeStream(Path(output.file), ZStream.fromChunk(data.chunk)))
}
