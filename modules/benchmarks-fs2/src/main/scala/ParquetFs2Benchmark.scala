package me.mnedokushev.zio.apache.parquet.benchmarks

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.mjakubowski84.parquet4s.{ parquet, Path as P4sPath }
import me.mnedokushev.zio.apache.parquet.hadoop.Path
import org.openjdk.jmh.annotations.*
import zio.ZIO

import ParquetBenchmarkSupport.*

object ParquetFs2Benchmark {
  @State(Scope.Thread)
  class StreamState {
    var runtime: IORuntime       = scala.compiletime.uninitialized
    var chunk: fs2.Chunk[Record] = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(data: Dataset): Unit = {
      runtime = IORuntime.global
      chunk = fs2.Chunk.from(data.records)
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmark = new ParquetFs2Benchmark
    val state     = new StreamState
    verify(
      data => run(ZIO.scoped(data.zioReader.readStream(Path(data.input)).runCollect)),
      benchmark.zioStreamingWrite
    )
    verify(
      data => {
        state.setup(data)
        benchmark.read(data).compile.toVector.unsafeRunSync()(using state.runtime)
      },
      (data, output) => benchmark.fs2StreamingWrite(data, output, state)
    )
  }
}

class ParquetFs2Benchmark extends ParquetStreamingBenchmark {
  private def read(data: Dataset) =
    parquet.fromParquet[IO].as[Record].options(data.parquet4sReaderOptions).read(P4sPath(data.input))

  @Benchmark
  def fs2StreamingRead(data: Dataset, state: ParquetFs2Benchmark.StreamState): Record =
    read(data).compile.lastOrError.unsafeRunSync()(using state.runtime)

  @Benchmark
  def fs2StreamingWrite(data: Dataset, output: Output, state: ParquetFs2Benchmark.StreamState): Unit =
    fs2.Stream
      .chunk(state.chunk)
      .through(parquet.writeSingleFile[IO].of[Record].options(data.parquet4sOptions).write(P4sPath(output.file)))
      .compile
      .drain
      .unsafeRunSync()(using state.runtime)
}
