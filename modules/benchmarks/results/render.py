"""Render the four Parquet JMH JSON files as a Markdown comparison table."""

import argparse
import json
import math
from pathlib import Path


SUITES = (
    ("batch.json", "Core", "Batch", "Batch", "parquet4s"),
    ("fs2.json", "FS2", "Stream", "Streaming", "fs2"),
    ("akka.json", "Akka", "Stream", "Streaming", "akka"),
    ("pekko.json", "Pekko", "Stream", "Streaming", "pekko"),
)
ROW_COUNTS = (10_000, 524_288)
DIRECTIONS = ("Read", "Write")


def read_scores(path):
    scores = {}
    for result in json.loads(path.read_text(encoding="utf-8")):
        method = result["benchmark"].rsplit(".", 1)[-1]
        rows = int(result["params"]["rows"])
        metric = result["primaryMetric"]
        score, error = metric["score"], metric["scoreError"]
        key = (method, rows)
        if key in scores:
            raise ValueError(f"{path.name}: duplicate result for {method} at {rows:,} rows")
        if result["mode"] != "avgt" or metric["scoreUnit"] != "ms/op":
            raise ValueError(f"{path.name}: expected average time in ms/op for {method}")
        if not (math.isfinite(score) and score > 0 and math.isfinite(error) and error >= 0):
            raise ValueError(f"{path.name}: invalid timing for {method} at {rows:,} rows")
        scores[key] = (score, error)
    return scores


def render(results_dir):
    lines = [
        "# Parquet benchmark results",
        "",
        "Times are mean milliseconds per file ± JMH’s 99.9% confidence margin.",
        "**Factor** is parquet4s time divided by ZIO time from the same suite.",
        "",
        "| parquet4s suite | Operation | Rows | ZIO ms/file | parquet4s ms/file | Factor |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for filename, suite, operation, method_kind, adapter in SUITES:
        scores = read_scores(results_dir / filename)
        for direction in DIRECTIONS:
            for rows in ROW_COUNTS:
                zio, zio_error = scores.pop((f"zio{method_kind}{direction}", rows))
                parquet4s, parquet4s_error = scores.pop((f"{adapter}{method_kind}{direction}", rows))
                lines.append(
                    f"| {suite} | {operation} {direction.lower()} | {rows:,} | "
                    f"{zio:.3f} ± {zio_error:.3f} | {parquet4s:.3f} ± {parquet4s_error:.3f} | "
                    f"{parquet4s / zio:.2f}× |"
                )
        if scores:
            raise ValueError(f"{filename}: unexpected results for {list(scores)}")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="directory containing the four JMH JSON files")
    args = parser.parse_args()
    print(render(args.results_dir))
