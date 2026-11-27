# Parquet benchmark results

Times are mean milliseconds per file ± JMH’s 99.9% confidence margin.
**Factor** is parquet4s time divided by ZIO time from the same suite.

| parquet4s suite | Operation | Rows | ZIO ms/file | parquet4s ms/file | Factor |
| --- | --- | ---: | ---: | ---: | ---: |
| Core | Batch read | 10,000 | 2.909 ± 0.194 | 5.275 ± 1.867 | 1.81× |
| Core | Batch read | 524,288 | 124.880 ± 3.470 | 180.725 ± 15.491 | 1.45× |
| Core | Batch write | 10,000 | 9.363 ± 0.447 | 9.724 ± 0.315 | 1.04× |
| Core | Batch write | 524,288 | 236.048 ± 2.269 | 283.292 ± 2.233 | 1.20× |
| FS2 | Stream read | 10,000 | 2.961 ± 0.333 | 27.490 ± 3.963 | 9.28× |
| FS2 | Stream read | 524,288 | 124.779 ± 2.175 | 1084.520 ± 39.011 | 8.69× |
| FS2 | Stream write | 10,000 | 10.135 ± 0.458 | 14.720 ± 1.093 | 1.45× |
| FS2 | Stream write | 524,288 | 239.052 ± 1.684 | 572.433 ± 36.551 | 2.39× |
| Akka | Stream read | 10,000 | 2.941 ± 0.294 | 15.847 ± 0.986 | 5.39× |
| Akka | Stream read | 524,288 | 125.247 ± 4.922 | 653.700 ± 8.191 | 5.22× |
| Akka | Stream write | 10,000 | 10.119 ± 0.477 | 11.385 ± 0.577 | 1.13× |
| Akka | Stream write | 524,288 | 240.037 ± 2.210 | 340.036 ± 7.788 | 1.42× |
| Pekko | Stream read | 10,000 | 3.024 ± 0.397 | 15.617 ± 1.298 | 5.16× |
| Pekko | Stream read | 524,288 | 121.012 ± 1.285 | 625.967 ± 10.589 | 5.17× |
| Pekko | Stream write | 10,000 | 10.338 ± 0.396 | 11.360 ± 0.491 | 1.10× |
| Pekko | Stream write | 524,288 | 241.960 ± 3.064 | 332.830 ± 3.197 | 1.38× |
