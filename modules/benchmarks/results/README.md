# Parquet benchmark results

## 2026-11-26 · Ryzen 9 7950X

Local Linux x86-64 run of revision `c44e9d5` with JDK 17.0.21 and parquet4s
2.23.0. The four JMH suites ran sequentially with one thread, two forks, five
one-second warmup iterations, and eight one-second measurement iterations per
fork.

[Run metadata](local/2026-11-26-ryzen-7950x/metadata.json) ·
[Core JSON](local/2026-11-26-ryzen-7950x/batch.json) ·
[FS2 JSON](local/2026-11-26-ryzen-7950x/fs2.json) ·
[Akka JSON](local/2026-11-26-ryzen-7950x/akka.json) ·
[Pekko JSON](local/2026-11-26-ryzen-7950x/pekko.json)

Times are mean milliseconds per file ± JMH’s 99.9% confidence margin.
**Factor** is parquet4s time divided by ZIO time from the same suite. A factor
of 2× means parquet4s took twice as long.

| parquet4s suite | Operation | Rows | ZIO ms/file | parquet4s ms/file | Factor |
| --- | --- | ---: | ---: | ---: | ---: |
| Core | Batch read | 10,000 | 1.986 ± 0.042 | 2.997 ± 0.106 | 1.51× |
| Core | Batch read | 524,288 | 97.784 ± 6.480 | 100.216 ± 9.609 | 1.02× |
| Core | Batch write | 10,000 | 6.595 ± 0.304 | 7.126 ± 0.211 | 1.08× |
| Core | Batch write | 524,288 | 189.901 ± 5.416 | 266.987 ± 3.300 | 1.41× |
| FS2 | Stream read | 10,000 | 2.382 ± 0.165 | 13.766 ± 0.227 | 5.78× |
| FS2 | Stream read | 524,288 | 102.212 ± 4.077 | 610.020 ± 7.186 | 5.97× |
| FS2 | Stream write | 10,000 | 7.014 ± 0.348 | 10.894 ± 0.385 | 1.55× |
| FS2 | Stream write | 524,288 | 195.695 ± 3.717 | 438.581 ± 29.376 | 2.24× |
| Akka | Stream read | 10,000 | 2.165 ± 0.157 | 10.816 ± 0.631 | 4.99× |
| Akka | Stream read | 524,288 | 95.615 ± 2.268 | 453.874 ± 20.487 | 4.75× |
| Akka | Stream write | 10,000 | 6.609 ± 0.120 | 8.180 ± 0.331 | 1.24× |
| Akka | Stream write | 524,288 | 196.741 ± 2.780 | 315.740 ± 25.735 | 1.60× |
| Pekko | Stream read | 10,000 | 2.050 ± 0.154 | 10.966 ± 0.619 | 5.35× |
| Pekko | Stream read | 524,288 | 96.480 ± 2.305 | 439.455 ± 29.542 | 4.55× |
| Pekko | Stream write | 10,000 | 6.757 ± 0.208 | 7.987 ± 0.219 | 1.18× |
| Pekko | Stream write | 524,288 | 196.629 ± 2.214 | 296.799 ± 8.306 | 1.51× |

The parquet4s streaming adapters took 4.55–5.97× as long as ZIO to read and
1.18–2.24× as long to write. The 524,288-row batch read differed by about 2%,
with overlapping confidence intervals. At that row count, the batch write
averaged 190 ms for ZIO and 267 ms for parquet4s core.

This snapshot is a local historical reference. Establish the CI regression
baseline on the runner profile used for CI comparisons.
