# Parquet I/O benchmarks

Run the Scala 3 JMH benchmarks from the repository root with JDK 17.

```sh
sbt 'benchmarks/Jmh/run .*ParquetBatchBenchmark.*'
sbt 'benchmarks/Jmh/run .*ParquetFilterBenchmark.*'
sbt 'benchmarksFs2/Jmh/run .*ParquetFs2Benchmark.*'
sbt 'benchmarksAkka/Jmh/run .*ParquetAkkaBenchmark.*'
sbt 'benchmarksPekko/Jmh/run .*ParquetPekkoBenchmark.*'
```

The **Parquet Benchmarks** CI job runs all four suites sequentially on each push
to `main`. Open the workflow run summary for the comparison table, or download
the benchmark artifact for the Markdown report and raw JMH JSON. The
`benchmark-results` branch keeps a copy of each report, raw results, and runner
metadata under `<commit SHA>/<run ID>-<run attempt>/`.

Each streaming adapter runs in its own sbt project. This also keeps the Akka
and Pekko adapters on separate classpaths, since they publish classes with the
same fully qualified names.

The batch suite compares ZIO `readChunk` and `writeChunk`, parquet4s 2.23.0
core, and the Apache Parquet Java API. All three readers consume the same
parquet4s file and return a `Chunk` of typed records. All three writers
start from the same typed `Chunk`. The Java methods use Parquet's `Group`
model with handwritten mapping. The Java writer uses the same schema as
the ZIO writer, prepared at trial setup. parquet4s uses its own derived
schema, which can differ in whether fields are marked required. That
encoding difference is part of the end-to-end write measurement.

The `shape` parameter selects `narrow` (the original fixture), `wideFlat`
(18 top-level columns), or `wideNested` (the same 18 leaf columns and values,
with up to three levels of groups). For example, pass `-p shape=wideNested`
to run only the nested cases.

The batch filter suite uses the wide-flat fixture and compares ZIO
`readChunkFiltered` with parquet4s core pushdown and with a full ZIO read
followed by in-memory filtering. All three methods return the matching
records in a `Chunk`.

Each streaming suite compares ZIO with the corresponding parquet4s 2.23.0
adapter. Unfiltered reads and writes support all three `shape` values.
Filtered reads compare ZIO `readStreamFiltered` with the adapter's
pushdown on the same wide-flat fixtures used by the batch filter suite.
Streaming readers consume the entire stream and return the last emitted
record. Writes stream preloaded records into a single file and wait for
completion.

For filtered reads, the same equality predicate selects roughly 1% of rows
in both layouts. Select a layout with `-p layout=clustered` or
`-p layout=scattered` when running filtered methods.
The `clustered` file puts matches first and the `scattered` file puts a
match at every hundredth row. Setup checks the Parquet footer to confirm
that row-group min/max statistics leave substantially fewer groups eligible
in the clustered layout. The ZIO predicate is compiled at trial setup.

JMH reports average milliseconds per file, including opening and closing it.
All suites use deterministic fixtures at 10,000 and 524,288 rows, with
uncompressed output, dictionary encoding, 32 KiB pages, and 64 KiB row
groups. Dataset generation, actor-system startup and shutdown, and output
cleanup happen outside the measured operations. Reads use the local
filesystem with normal OS caching.

Run these correctness checks to verify record values and order. The batch
check reads each writer's output with all three readers and checks column
paths and types. The batch and streaming filter checks compare every read
path with the expected matches and print the candidate row-group counts.
The unfiltered streaming checks use parquet4s core to generate the input
fixtures and to check the files produced by the streaming writers.

```sh
sbt 'benchmarks/runMain me.mnedokushev.zio.apache.parquet.benchmarks.ParquetBatchBenchmark' \
    'benchmarks/runMain me.mnedokushev.zio.apache.parquet.benchmarks.ParquetFilterBenchmark' \
    'benchmarksFs2/runMain me.mnedokushev.zio.apache.parquet.benchmarks.ParquetFs2Benchmark' \
    'benchmarksAkka/runMain me.mnedokushev.zio.apache.parquet.benchmarks.ParquetAkkaBenchmark' \
    'benchmarksPekko/runMain me.mnedokushev.zio.apache.parquet.benchmarks.ParquetPekkoBenchmark'
```

For a quick JMH smoke run, add `-foe true -wi 0 -i 1 -r 100ms -f 1` before the
suite filter.

To run just the filtered reads for one adapter, use:

```sh
sbt 'benchmarksFs2/Jmh/run .*ParquetFs2Benchmark.*StreamingFilteredRead'
```

The narrow fixture's shape and distributions are based on the
[parquet4s Core fixture](https://github.com/mjakubowski84/parquet4s/blob/33de59f77f88fd3c6975217ac824bfa78f74a714/coreBenchmarks/src/main/scala/com/github/mjakubowski84/parquet4s/CoreBenchmark.scala).
