# Parquet I/O benchmarks

Run the Scala 3 JMH suite from the repository root with JDK 17.

```sh
sbt 'benchmarks/Jmh/run .*ParquetBenchmark.*'
```

For a quick smoke run across both row counts and all eight methods:

```sh
sbt 'benchmarks/Jmh/run -foe true -wi 0 -i 1 -r 100ms -f 1 .*ParquetBenchmark.*'
```

| Workload | zio-apache-parquet | parquet4s 2.23.0 |
| --- | --- | --- |
| Batch read | `readChunk` | Core reader collected into a `Chunk` |
| Batch write | `writeChunk` | Core `writeAndClose` |
| Streaming read | `readStream` consumed to the last row | FS2 reader consumed to the last row |
| Streaming write | `writeStream` | FS2 single-file writer |

JMH reports average milliseconds per operation. Each read uses the same deterministic
input at its row count (10,000 or 524,288). Both writers use the same rows, uncompressed
output, dictionary encoding, 32 KiB pages, and 64 KiB row groups. Timing includes opening
and closing the file. Dataset generation and output cleanup run outside the measured
operation. Reads use the local filesystem with normal OS caching.

The data shape and distributions are based on the
[parquet4s Core fixture](https://github.com/mjakubowski84/parquet4s/blob/33de59f77f88fd3c6975217ac824bfa78f74a714/coreBenchmarks/src/main/scala/com/github/mjakubowski84/parquet4s/CoreBenchmark.scala).
