#!/usr/bin/env bash
set -euo pipefail

results_dir=$1
history_ref=refs/heads/benchmark-results
run_dir="$GITHUB_SHA/$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
archive_dir=$(mktemp -d)
trap 'rm -rf -- "$archive_dir"' EXIT

# Use a temporary index so the archive has its own history and checkout files stay in place.
export GIT_INDEX_FILE="$archive_dir/index"
mkdir -p "$archive_dir/files/$run_dir"
cp -- "$results_dir"/{README.md,batch.json,fs2.json,akka.json,pekko.json,metadata.json} \
  "$archive_dir/files/$run_dir/"

for attempt in 1 2 3 4 5; do
  parent_args=()
  existing_ref=$(git ls-remote --refs origin "$history_ref")
  if [[ -n $existing_ref ]]; then
    git fetch --no-tags origin "$history_ref"
    parent=$(git rev-parse FETCH_HEAD)
    if [[ -n $(git ls-tree "$parent" -- "$run_dir") ]]; then
      printf 'Benchmark run already archived at %s\n' "$run_dir"
      exit 0
    fi
    git read-tree "$parent"
    parent_args=(-p "$parent")
  else
    git read-tree --empty
  fi

  git --work-tree="$archive_dir/files" add -- "$run_dir"
  tree=$(git write-tree)
  commit=$(git -c user.name='github-actions[bot]' \
    -c user.email='41898282+github-actions[bot]@users.noreply.github.com' \
    commit-tree "$tree" "${parent_args[@]}" -m "Archive benchmark run $run_dir")

  if git push origin "$commit:$history_ref"; then
    exit 0
  fi

  # A concurrent run may have updated the branch. Fetch again and append to its new tip.
  if (( attempt < 5 )); then
    sleep "$attempt"
  fi
done

printf 'Could not archive benchmark results on %s\n' "$history_ref" >&2
exit 1
