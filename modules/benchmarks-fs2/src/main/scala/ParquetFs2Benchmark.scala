package me.mnedokushev.zio.apache.parquet.benchmarks

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.mjakubowski84.parquet4s.{ parquet, Path as P4sPath }
import org.openjdk.jmh.annotations.*

import ParquetBenchmarkSupport.*
import ParquetFilterBenchmark.{ verifyFilteredStreaming, FilterDataset }
import ParquetWideRecords.WideFlat

object ParquetFs2Benchmark {
  @State(Scope.Thread)
  class StreamRuntime {
    val runtime: IORuntime = IORuntime.global
  }

  private[benchmarks] class WriteInput[A <: Product](val file: StreamFile[A]) {
    val chunk: fs2.Chunk[A] = fs2.Chunk.from(file.records)
  }

  @State(Scope.Thread)
  class WriteState {
    private[benchmarks] var input: WriteInput[?] = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(data: Dataset): Unit =
      input = new WriteInput(data.file)
  }

  def main(args: Array[String]): Unit = {
    val benchmark   = new ParquetFs2Benchmark
    given IORuntime = IORuntime.global
    verifyStreaming(
      [A <: Product] => (file: StreamFile[A]) => benchmark.read(file).compile.toVector.unsafeRunSync(),
      [A <: Product] => (file: StreamFile[A], output: Output) => benchmark.write(new WriteInput(file), output)
    )
    verifyFilteredStreaming(data => benchmark.readFiltered(data).compile.toVector.unsafeRunSync())
  }
}

class ParquetFs2Benchmark extends ParquetStreamingBenchmark {
  import ParquetFs2Benchmark.*

  private def read[A <: Product](data: StreamFile[A]): fs2.Stream[IO, A] =
    parquet
      .fromParquet[IO]
      .as[A](using data.parquet4sDecoder)
      .options(data.parquet4sReaderOptions)
      .read(P4sPath(data.input))

  private def readFiltered(data: FilterDataset): fs2.Stream[IO, WideFlat] =
    parquet
      .fromParquet[IO]
      .as[WideFlat]
      .options(data.parquet4sReaderOptions)
      .filter(data.parquet4sPredicate)
      .read(P4sPath(data.input))

  @Benchmark
  def fs2StreamingRead(data: Dataset, state: StreamRuntime): Product =
    read(data.file).compile.lastOrError.unsafeRunSync()(using state.runtime)

  @Benchmark
  def fs2StreamingFilteredRead(data: FilterDataset, state: StreamRuntime): WideFlat =
    readFiltered(data).compile.lastOrError.unsafeRunSync()(using state.runtime)

  @Benchmark
  def fs2StreamingWrite(output: Output, state: WriteState, runtime: StreamRuntime): Unit =
    write(state.input, output)(using runtime.runtime)

  private def write[A <: Product](input: WriteInput[A], output: Output)(using IORuntime): Unit = {
    val file = input.file
    val pipe = parquet
      .writeSingleFile[IO]
      .of[A](using file.parquet4sSchema, file.parquet4sEncoder)
      .options(file.parquet4sOptions)
      .write(P4sPath(output.file))
    fs2.Stream.chunk(input.chunk).through(pipe).compile.drain.unsafeRunSync()
  }
}
