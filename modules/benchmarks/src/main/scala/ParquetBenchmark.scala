package me.mnedokushev.zio.apache.parquet.benchmarks

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.mjakubowski84.parquet4s.{
  parquet,
  ParquetReader as P4sReader,
  ParquetWriter as P4sWriter,
  Path as P4sPath
}
import me.mnedokushev.zio.apache.parquet.core.codec.*
import me.mnedokushev.zio.apache.parquet.hadoop.{ ParquetReader, ParquetWriter, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.openjdk.jmh.annotations.*
import zio.{ Chunk, Runtime, Unsafe, ZIO }
import zio.schema.{ Derive, DeriveSchema, Schema }
import zio.stream.ZStream

import java.nio.file.{ Files, Path as NioPath }
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.{ Random, Using }

object ParquetBenchmark {

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

  private def localConfiguration(): Configuration = {
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "file:///")
    configuration
  }

  private def run[A](effect: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(effect).getOrThrowFiberFailure()
    }

  private def deleteDirectory(directory: NioPath): Unit =
    Using.resource(Files.walk(directory)) { paths =>
      paths.sorted(java.util.Comparator.reverseOrder[NioPath]()).forEach(p => Files.delete(p))
    }

  @State(Scope.Thread)
  class Dataset {
    @Param(Array("10000", "524288"))
    var rows: Int = 0

    private var directory: NioPath                = scala.compiletime.uninitialized
    var records: Vector[Record]                   = scala.compiletime.uninitialized
    var chunk: Chunk[Record]                      = scala.compiletime.uninitialized
    var fs2Chunk: fs2.Chunk[Record]               = scala.compiletime.uninitialized
    var input: NioPath                            = scala.compiletime.uninitialized
    var zioReader: ParquetReader[Record]          = scala.compiletime.uninitialized
    var zioWriter: ParquetWriter[Record]          = scala.compiletime.uninitialized
    var parquet4sOptions: P4sWriter.Options       = scala.compiletime.uninitialized
    var parquet4sReaderOptions: P4sReader.Options = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(): Unit = {
      val random        = new Random(42L)
      records = Vector.tabulate(rows) { index =>
        val id       = index + 1
        val dict     = ('a' + random.nextInt(3)).toChar.toString
        val embedded = Option.when(id % 2 == 0) {
          Embedded(1.0 / 100.12, new UUID(random.nextLong(), random.nextLong()).toString)
        }
        Record(id, dict, embedded)
      }
      chunk = Chunk.fromIterable(records)
      fs2Chunk = fs2.Chunk.from(records)
      directory = Files.createTempDirectory("parquet-read-")
      input = directory.resolve("input.parquet")
      val configuration = localConfiguration()
      parquet4sOptions = P4sWriter.Options(
        writeMode = ParquetFileWriter.Mode.CREATE,
        compressionCodecName = UNCOMPRESSED,
        dictionaryEncodingEnabled = true,
        pageSize = pageSize,
        rowGroupSize = rowGroupSize,
        hadoopConf = configuration
      )
      parquet4sReaderOptions = P4sReader.Options(hadoopConf = configuration)
      zioReader = run(ZIO.scoped(ParquetReader.configured[Record](configuration).build.map(_.get)))
      zioWriter = run(
        ZIO.scoped(
          ParquetWriter
            .configured[Record](
              writeMode = ParquetFileWriter.Mode.CREATE,
              compressionCodecName = UNCOMPRESSED,
              dictionaryEncodingEnabled = true,
              pageSize = pageSize,
              rowGroupSize = rowGroupSize,
              hadoopConf = configuration
            )
            .build
            .map(_.get)
        )
      )
      P4sWriter.of[Record].options(parquet4sOptions).writeAndClose(P4sPath(input), records)
    }

    @TearDown(Level.Trial)
    def cleanup(): Unit = deleteDirectory(directory)
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
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(2)
@Threads(1)
class ParquetBenchmark {
  import ParquetBenchmark.*

  @Benchmark
  def zioBatchRead(data: Dataset): Chunk[Record] =
    run(data.zioReader.readChunk[Record](Path(data.input)))

  @Benchmark
  def parquet4sBatchRead(data: Dataset): Chunk[Record] =
    Using.resource(P4sReader.as[Record].options(data.parquet4sReaderOptions).read(P4sPath(data.input))) { rows =>
      val builder = zio.ChunkBuilder.make[Record]()
      rows.iterator.foreach(row => builder += row)
      builder.result()
    }

  @Benchmark
  def zioBatchWrite(data: Dataset, output: Output): Unit =
    run(data.zioWriter.writeChunk(Path(output.file), data.chunk))

  @Benchmark
  def parquet4sBatchWrite(data: Dataset, output: Output): Unit =
    P4sWriter.of[Record].options(data.parquet4sOptions).writeAndClose(P4sPath(output.file), data.chunk)

  @Benchmark
  def zioStreamingRead(data: Dataset): Record =
    run(ZIO.scoped(data.zioReader.readStream(Path(data.input)).runLast)).get

  @Benchmark
  def parquet4sStreamingRead(data: Dataset): Record =
    parquet
      .fromParquet[IO]
      .as[Record]
      .options(data.parquet4sReaderOptions)
      .read(P4sPath(data.input))
      .compile
      .lastOrError
      .unsafeRunSync()

  @Benchmark
  def zioStreamingWrite(data: Dataset, output: Output): Unit =
    run(data.zioWriter.writeStream(Path(output.file), ZStream.fromChunk(data.chunk)))

  @Benchmark
  def parquet4sStreamingWrite(data: Dataset, output: Output): Unit =
    fs2.Stream
      .chunk(data.fs2Chunk)
      .through(parquet.writeSingleFile[IO].of[Record].options(data.parquet4sOptions).write(P4sPath(output.file)))
      .compile
      .drain
      .unsafeRunSync()
}
