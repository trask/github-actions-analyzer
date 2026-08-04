"""Three checks from cached data: the ~119 min ceiling, whether the go control is
real, and whether burst shape explains which repos stall."""

import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github_actions_analyzer import (  # noqa: E402
    AnalyzerConfig,
    CachingHttpClient,
    JobRecord,
    _is_hosted,
    _percentile,
    get_workflow_data,
)

START = datetime(2026, 6, 29, tzinfo=UTC)
END = datetime(2026, 8, 1, tzinfo=UTC)
REPOS = ("semantic-conventions", "opentelemetry-js", "opentelemetry-go")
BIN = timedelta(minutes=10)


def _minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60


# _percentile indexes a pre-sorted list, so never hand it raw values
def _pct(values: list[float], percentile: float) -> float:
    return _percentile(sorted(values), percentile)


def main() -> None:
    config = AnalyzerConfig(
        org="open-telemetry",
        start=START,
        end=END,
        min_queue_time=timedelta(0),
        workflow_timeout=timedelta(hours=6),
        tz_offset=timedelta(0),
    )
    client = CachingHttpClient(cache_path=Path("cache.sqlite"), print_progress=False)
    try:
        by_repo: dict[str, list[JobRecord]] = {}
        for repo in REPOS:
            records = get_workflow_data(client, config, repo, print_jobs=False)
            by_repo[repo] = [r for r in records if _is_hosted(r.labels)]
    finally:
        client.close()

    print("\n=== CHECK 1: is there a ceiling near 119 min? ===")
    for repo, records in by_repo.items():
        tail = sorted(records, key=lambda r: -r.queue_duration)[:12]
        print(f"{repo:24} top waits: {', '.join(f'{_minutes(r.queue_duration):.1f}' for r in tail)}")
        # a tight cluster of waits means either a cap or one queue draining at once
        print(f"{'':24}   created -> started (UTC) for those jobs:")
        for record in tail:
            started = record.created_at + record.queue_duration
            print(f"{'':24}   {record.created_at:%m-%d %H:%M:%S} -> {started:%m-%d %H:%M:%S}  {record.html_url}")
    buckets = Counter()
    for records in by_repo.values():
        for record in records:
            minutes = _minutes(record.queue_duration)
            if minutes >= 60:
                buckets[int(minutes // 10) * 10] += 1
    print("\nall repos, waits >= 60 min, by 10-min bucket:")
    for low in sorted(buckets):
        print(f"  {low:3}-{low + 10:3} min: {buckets[low]:4}")

    print("\n=== CHECK 2: is the go control real? ===")
    for repo, records in by_repo.items():
        waits = [_minutes(r.queue_duration) for r in records]
        over5 = sum(1 for w in waits if w > 5)
        print(
            f"{repo:24} n={len(waits):6} p50={_pct(waits, 50):6.1f} p90={_pct(waits, 90):6.1f} "
            f"p99={_pct(waits, 99):6.1f} p99.9={_pct(waits, 99.9):6.1f} max={max(waits):6.1f} "
            f">5m={over5:5} ({over5 / len(waits):.1%})"
        )

    print("\n=== CHECK 3: burst shape ===")
    for repo, records in by_repo.items():
        bins: dict[datetime, list[float]] = defaultdict(list)
        for record in records:
            key = record.created_at - (record.created_at - START) % BIN
            bins[key].append(_minutes(record.queue_duration))
        sizes = [float(len(v)) for v in bins.values()]
        print(
            f"{repo:24} bins={len(bins):5} max_burst={int(max(sizes)):5} "
            f"p99_burst={_pct(sizes, 99):6.1f} median_burst={_pct(sizes, 50):5.1f}"
        )
        worst = sorted(bins.items(), key=lambda kv: -_pct(kv[1], 90))[:5]
        for key, waits in worst:
            print(f"    worst bin {key:%Y-%m-%d %H:%M}  jobs={len(waits):5}  p90={_pct(waits, 90):6.1f}")
        biggest = sorted(bins.items(), key=lambda kv: -len(kv[1]))[:5]
        for key, waits in biggest:
            print(f"    biggest bin {key:%Y-%m-%d %H:%M}  jobs={len(waits):5}  p90={_pct(waits, 90):6.1f}")


if __name__ == "__main__":
    main()
