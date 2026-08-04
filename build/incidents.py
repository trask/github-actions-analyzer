"""Enumerate every queue-wait incident across the cached window so recurrence can
be shown, with the other repos measured in the exact same minutes as controls."""

import math
import sys
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
REPOS = (
    "semantic-conventions",
    "opentelemetry-python",
    "opentelemetry-python-contrib",
    "opentelemetry-js",
    "opentelemetry-go",
)
BIN = timedelta(minutes=10)
STALL_P90 = 10.0
MIN_JOBS = 5
MERGE_GAP = timedelta(minutes=30)


def _minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60


def _pct(values: list[float], percentile: float) -> float:
    return _percentile(sorted(values), percentile)


# the record sitting at the percentile, so the reported number links to the job it came from
def _pct_record(records: list[JobRecord], percentile: float) -> JobRecord:
    ordered = sorted(records, key=lambda record: record.queue_duration)
    index = math.ceil(percentile / 100 * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def _bin_of(moment: datetime) -> datetime:
    return moment - (moment - START) % BIN


def load_bins() -> dict[str, dict[datetime, list[JobRecord]]]:
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

    bins: dict[str, dict[datetime, list[JobRecord]]] = {}
    for repo, records in by_repo.items():
        per_bin: dict[datetime, list[JobRecord]] = {}
        for record in records:
            per_bin.setdefault(_bin_of(record.created_at), []).append(record)
        bins[repo] = per_bin
    return bins


def find_episodes(bins: dict[str, dict[datetime, list[JobRecord]]]) -> list[tuple[datetime, datetime]]:
    stalled: set[datetime] = set()
    for per_bin in bins.values():
        for key, binned in per_bin.items():
            if len(binned) >= MIN_JOBS and _pct([_minutes(r.queue_duration) for r in binned], 90) > STALL_P90:
                stalled.add(key)

    episodes: list[tuple[datetime, datetime]] = []
    for key in sorted(stalled):
        if episodes and key - episodes[-1][1] <= MERGE_GAP:
            episodes[-1] = (episodes[-1][0], key + BIN)
        else:
            episodes.append((key, key + BIN))
    return episodes


def in_window(
    bins: dict[str, dict[datetime, list[JobRecord]]],
    repo: str,
    start: datetime,
    end: datetime,
) -> list[JobRecord]:
    return [record for key, binned in bins[repo].items() if start <= key < end for record in binned]


def main() -> None:
    bins = load_bins()
    incidents = find_episodes(bins)

    short_names = [repo.replace("opentelemetry-", "").replace("semantic-conventions", "semantic-conv") for repo in REPOS]

    print(f"{len(incidents)} incidents over {(END - START).days} days\n")
    print("| incident (UTC) | mins | " + " | ".join(short_names) + " |")
    print("|---|---|" + "---|" * len(REPOS))

    for start, end in incidents:
        cells = []
        for repo in REPOS:
            records = in_window(bins, repo, start, end)
            if not records:
                cells.append("-")
                continue
            worst = _pct_record(records, 90)
            p90 = f"{_minutes(worst.queue_duration):.1f}"
            if _minutes(worst.queue_duration) > STALL_P90:
                p90 = f"**{p90}**"
            cells.append(f"{len(records)} / [{p90}]({worst.html_url})")
        print(f"| {start:%m-%d %H:%M} | {_minutes(end - start):.0f} | " + " | ".join(cells) + " |")

    print("\n| repository | stalled in | had jobs running in |")
    print("|---|---|---|")
    for repo in REPOS:
        hit = sum(
            1
            for start, end in incidents
            for records in [in_window(bins, repo, start, end)]
            if records and _pct([_minutes(r.queue_duration) for r in records], 90) > STALL_P90
        )
        active = sum(
            1
            for start, end in incidents
            if any(start <= key < end for key in bins[repo])
        )
        print(f"| {repo} | {hit} / {len(incidents)} | {active} |")


if __name__ == "__main__":
    main()
