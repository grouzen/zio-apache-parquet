package me.mnedokushev.zio.apache.parquet.benchmarks

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{ Materializer, SystemMaterializer }
import org.apache.pekko.stream.scaladsl.{ Sink, Source }
import com.github.mjakubowski84.parquet4s.{ ParquetStreams, Path as P4sPath }
import org.openjdk.jmh.annotations.*

import scala.concurrent.Await
import scala.concurrent.duration.*

import ParquetBenchmarkSupport.*

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
    try
      verify(
        data => Await.result(benchmark.read(data).runWith(Sink.seq)(using runtime.materializer), Duration.Inf),
        (data, output) => benchmark.pekkoStreamingWrite(data, output, runtime)
      )
    finally runtime.cleanup()
  }
}

class ParquetPekkoBenchmark extends ParquetStreamingBenchmark {
  import ParquetPekkoBenchmark.StreamRuntime

  private def read(data: Dataset) =
    ParquetStreams.fromParquet.as[Record].options(data.parquet4sReaderOptions).read(P4sPath(data.input))

  @Benchmark
  def pekkoStreamingRead(data: Dataset, runtime: StreamRuntime): Record =
    Await.result(read(data).runWith(Sink.last)(using runtime.materializer), Duration.Inf)

  @Benchmark
  def pekkoStreamingWrite(data: Dataset, output: Output, runtime: StreamRuntime): Unit = {
    val sink =
      ParquetStreams.toParquetSingleFile.of[Record].options(data.parquet4sOptions).write(P4sPath(output.file))
    val _    =
      Await.result(
        Source.fromIterator(() => data.records.iterator).runWith(sink)(using runtime.materializer),
        Duration.Inf
      )
  }
}
