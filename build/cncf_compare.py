"""Compare GitHub-hosted queue waits in other CNCF orgs against open-telemetry's
stall windows, to test whether the constraint is above the organisation."""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github_actions_analyzer import (  # noqa: E402
    AnalyzerConfig,
    CachingHttpClient,
    _is_hosted,
    _percentile,
    get_workflow_data,
)

WINDOWS = (
    ("STALL 07-20 14:30-18:30", datetime(2026, 7, 20, 14, 30, tzinfo=UTC), datetime(2026, 7, 20, 18, 30, tzinfo=UTC)),
    ("STALL 07-27 16:20-20:50", datetime(2026, 7, 27, 16, 20, tzinfo=UTC), datetime(2026, 7, 27, 20, 50, tzinfo=UTC)),
    ("STALL 07-31 15:20-17:20", datetime(2026, 7, 31, 15, 20, tzinfo=UTC), datetime(2026, 7, 31, 17, 20, tzinfo=UTC)),
    ("CLEAN 07-23 14:30-18:30", datetime(2026, 7, 23, 14, 30, tzinfo=UTC), datetime(2026, 7, 23, 18, 30, tzinfo=UTC)),
    ("CLEAN 07-29 15:00-18:00", datetime(2026, 7, 29, 15, 0, tzinfo=UTC), datetime(2026, 7, 29, 18, 0, tzinfo=UTC)),
)

TARGETS = (
    ("open-telemetry", "semantic-conventions"),
    ("open-telemetry", "opentelemetry-python-contrib"),
    ("open-telemetry", "opentelemetry-go"),
    ("cilium", "cilium"),
    ("vitessio", "vitess"),
    ("backstage", "backstage"),
    ("dapr", "dapr"),
    ("prometheus", "prometheus"),
    ("containerd", "containerd"),
)


def _minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60


def _pct(values: list[float], percentile: float) -> float:
    # _percentile indexes a pre-sorted list, so never hand it raw values
    return _percentile(sorted(values), percentile)


def main() -> None:
    client = CachingHttpClient(cache_path=Path("cache.sqlite"), print_progress=False)
    try:
        for title, start, end in WINDOWS:
            print(f"\n=== {title}")
            print(f"{'repo':<34}{'jobs':>7}{'p50':>8}{'p90':>8}{'max':>8}{'>10m':>7}")
            for org, repo in TARGETS:
                config = AnalyzerConfig(
                    org=org,
                    start=start,
                    end=end,
                    min_queue_time=timedelta(0),
                    workflow_timeout=timedelta(hours=6),
                    tz_offset=timedelta(0),
                )
                records = get_workflow_data(client, config, repo, print_jobs=False)
                waits = [
                    _minutes(record.queue_duration)
                    for record in records
                    if _is_hosted(record.labels) and start <= record.created_at < end
                ]
                if not waits:
                    print(f"{org + '/' + repo:<34}{'-':>7}")
                    continue
                over = sum(1 for wait in waits if wait > 10) / len(waits) * 100
                print(
                    f"{org + '/' + repo:<34}{len(waits):>7}"
                    f"{_pct(waits, 50):>8.1f}{_pct(waits, 90):>8.1f}{max(waits):>8.1f}{over:>6.0f}%"
                )
                sys.stdout.flush()
    finally:
        client.close()


if __name__ == "__main__":
    main()
