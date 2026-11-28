package me.mnedokushev.zio.apache.parquet.benchmarks

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{ Materializer, SystemMaterializer }
import org.apache.pekko.stream.scaladsl.{ Sink, Source }
import com.github.mjakubowski84.parquet4s.{ ParquetStreams, Path as P4sPath }
import org.openjdk.jmh.annotations.*

import scala.concurrent.Await
import scala.concurrent.duration.*

import ParquetBenchmarkSupport.*
import ParquetFilterBenchmark.{ verifyFilteredStreaming, FilterDataset }
import ParquetWideRecords.WideFlat

object ParquetPekkoBenchmark {
  @State(Scope.Thread)
  class StreamRuntime {
    private var system: ActorSystem = scala.compiletime.uninitialized
    var materializer: Materializer  = scala.compiletime.uninitialized

    @Setup(Level.Trial)
    def setup(): Unit = {
      system = ActorSystem("parquet-benchmark")
      materializer = SystemMaterializer(system).materializer
    }

    @TearDown(Level.Trial)
    def cleanup(): Unit = {
      val _ = Await.result(system.terminate(), 30.seconds)
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmark = new ParquetPekkoBenchmark
    val runtime   = new StreamRuntime
    runtime.setup()
    try {
      verifyStreaming(
        [A <: Product] =>
          (file: StreamFile[A]) =>
            Await.result(benchmark.read(file).runWith(Sink.seq)(using runtime.materializer), Duration.Inf),
        [A <: Product] => (file: StreamFile[A], output: Output) => benchmark.write(file, output, runtime)
      )
      verifyFilteredStreaming(data =>
        Await.result(benchmark.readFiltered(data).runWith(Sink.seq)(using runtime.materializer), Duration.Inf)
      )
    } finally runtime.cleanup()
  }
}

class ParquetPekkoBenchmark extends ParquetStreamingBenchmark {
  import ParquetPekkoBenchmark.StreamRuntime

  private def read[A <: Product](data: StreamFile[A]) =
    ParquetStreams.fromParquet
      .as[A](using data.parquet4sDecoder)
      .options(data.parquet4sReaderOptions)
      .read(P4sPath(data.input))

  private def readFiltered(data: FilterDataset) =
    ParquetStreams.fromParquet
      .as[WideFlat]
      .options(data.parquet4sReaderOptions)
      .filter(data.parquet4sPredicate)
      .read(P4sPath(data.input))

  @Benchmark
  def pekkoStreamingRead(data: Dataset, runtime: StreamRuntime): Product =
    Await.result(read(data.file).runWith(Sink.last)(using runtime.materializer), Duration.Inf)

  @Benchmark
  def pekkoStreamingFilteredRead(data: FilterDataset, runtime: StreamRuntime): WideFlat =
    Await.result(readFiltered(data).runWith(Sink.last)(using runtime.materializer), Duration.Inf)

  @Benchmark
  def pekkoStreamingWrite(data: Dataset, output: Output, runtime: StreamRuntime): Unit =
    write(data.file, output, runtime)

  private def write[A <: Product](data: StreamFile[A], output: Output, runtime: StreamRuntime): Unit = {
    val sink = ParquetStreams.toParquetSingleFile
      .of[A](using data.parquet4sSchema, data.parquet4sEncoder)
      .options(data.parquet4sOptions)
      .write(P4sPath(output.file))
    val _    = Await.result(
      Source.fromIterator(() => data.records.iterator).runWith(sink)(using runtime.materializer),
      Duration.Inf
    )
  }
}
