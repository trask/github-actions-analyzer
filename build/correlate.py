"""One-off: do python-repo and collector-contrib hosted jobs stall in the SAME minutes?

Shared-pool saturation or an org quota implies correlated waits across repos.
Per-repo scheduling or self-inflicted bursts imply uncorrelated waits.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import correlation

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github_actions_analyzer import (  # noqa: E402
    AnalyzerConfig,
    CachingHttpClient,
    JobRecord,
    _is_hosted,
    _percentile,
    get_workflow_data,
)

START = datetime(2026, 7, 27, tzinfo=UTC)
END = datetime(2026, 8, 1, tzinfo=UTC)
BIN = timedelta(minutes=10)
MIN_JOBS_PER_BIN = 20

GROUPS = {
    "python": ["opentelemetry-python", "opentelemetry-python-contrib"],
    "collector": ["opentelemetry-collector-contrib"],
}


def collect(client: CachingHttpClient, config: AnalyzerConfig, repos: list[str]) -> list[JobRecord]:
    records: list[JobRecord] = []
    for repo in repos:
        records.extend(get_workflow_data(client, config, repo, print_jobs=False))
    return [record for record in records if _is_hosted(record.labels)]


def binned(records: list[JobRecord]) -> dict[int, list[float]]:
    bins: dict[int, list[float]] = defaultdict(list)
    for record in records:
        index = int((record.created_at - START) / BIN)
        bins[index].append(record.queue_duration.total_seconds() / 60)
    return bins


def main() -> None:
    config = AnalyzerConfig(
        org="open-telemetry",
        start=START,
        end=END,
        min_queue_time=timedelta(0),
        workflow_timeout=timedelta(hours=6),
        tz_offset=timedelta(0),
    )
    client = CachingHttpClient(cache_path=Path("cache.sqlite"))
    try:
        bins = {name: binned(collect(client, config, repos)) for name, repos in GROUPS.items()}
    finally:
        client.close()

    shared = sorted(
        index
        for index in set(bins["python"]) & set(bins["collector"])
        if len(bins["python"][index]) >= MIN_JOBS_PER_BIN
        and len(bins["collector"][index]) >= MIN_JOBS_PER_BIN
    )
    print(f"shared 10-min bins with >= {MIN_JOBS_PER_BIN} hosted jobs each: {len(shared)}")
    if len(shared) < 3:
        return

    python_p90 = [_percentile(sorted(bins["python"][i]), 90) for i in shared]
    collector_p90 = [_percentile(sorted(bins["collector"][i]), 90) for i in shared]

    print(f"pearson  p90 correlation: {correlation(python_p90, collector_p90):+.3f}")
    print(f"spearman p90 correlation: {correlation(python_p90, collector_p90, method='ranked'):+.3f}")

    print("\nworst 15 python bins, with collector at the same moment:")
    print(f"{'bin start (UTC)':<20}  {'py p90':>7}  {'py jobs':>7}  {'col p90':>7}  {'col jobs':>8}")
    worst = sorted(range(len(shared)), key=lambda i: python_p90[i], reverse=True)[:15]
    for i in worst:
        start = START + shared[i] * BIN
        print(
            f"{start.strftime('%Y-%m-%d %H:%M'):<20}  {python_p90[i]:>7.1f}  "
            f"{len(bins['python'][shared[i]]):>7}  {collector_p90[i]:>7.1f}  "
            f"{len(bins['collector'][shared[i]]):>8}"
        )

    both_bad = sum(1 for i in range(len(shared)) if python_p90[i] > 10 and collector_p90[i] > 10)
    python_only = sum(1 for i in range(len(shared)) if python_p90[i] > 10 and collector_p90[i] <= 10)
    collector_only = sum(1 for i in range(len(shared)) if python_p90[i] <= 10 and collector_p90[i] > 10)
    print(f"\nbins p90>10m: both={both_bad}  python only={python_only}  collector only={collector_only}")


if __name__ == "__main__":
    main()
