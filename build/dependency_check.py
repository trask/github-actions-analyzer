"""Test whether stalled jobs were waiting on runners or on upstream jobs in their
own run. If a job starts right after a sibling completes, its "queue" time is
really `needs:` dependency wait."""

import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github_actions_analyzer import (  # noqa: E402
    AnalyzerConfig,
    CachingHttpClient,
    ServerError,
    _is_hosted,
    _parse_instant,
    get_jobs,
    get_workflow_runs,
)

WINDOWS = (
    ("2026-07-20", datetime(2026, 7, 20, 15, 30, tzinfo=UTC), datetime(2026, 7, 20, 17, 0, tzinfo=UTC)),
    ("2026-07-27", datetime(2026, 7, 27, 17, 30, tzinfo=UTC), datetime(2026, 7, 27, 19, 30, tzinfo=UTC)),
    ("2026-07-31", datetime(2026, 7, 31, 15, 30, tzinfo=UTC), datetime(2026, 7, 31, 18, 0, tzinfo=UTC)),
)
REPOS = ("semantic-conventions", "opentelemetry-js")
STALLED = timedelta(minutes=10)
CLOSE = timedelta(seconds=90)


def main() -> None:
    client = CachingHttpClient(cache_path=Path("cache.sqlite"), print_progress=False)
    try:
        for repo in REPOS:
            for window_name, start, end in WINDOWS:
                config = AnalyzerConfig(
                    org="open-telemetry",
                    start=start,
                    end=end,
                    min_queue_time=timedelta(0),
                    workflow_timeout=timedelta(hours=6),
                    tz_offset=timedelta(0),
                )
                blocked_by_sibling = 0
                no_sibling = 0
                names: Counter[str] = Counter()
                samples: list[str] = []

                for run in get_workflow_runs(client, config, repo):
                    if run["run_attempt"] > 1:
                        continue
                    try:
                        jobs = get_jobs(client, config.org, repo, run["id"], immutable=run.get("status") == "completed")
                    except ServerError:
                        continue

                    finishes = [
                        _parse_instant(j["completed_at"])
                        for j in jobs
                        if j.get("completed_at") and j.get("started_at")
                    ]

                    for job in jobs:
                        if job.get("started_at") is None or not job.get("runner_name"):
                            continue
                        labels = ",".join(job.get("labels") or ["(none)"])
                        if not _is_hosted(labels):
                            continue
                        created = _parse_instant(job["created_at"])
                        started = _parse_instant(job["started_at"])
                        if started - created < STALLED:
                            continue

                        names[job["name"][:40]] += 1
                        earlier = [f for f in finishes if f <= started]
                        gap = started - max(earlier) if earlier else None
                        if gap is not None and gap <= CLOSE:
                            blocked_by_sibling += 1
                            if len(samples) < 4:
                                samples.append(
                                    f"      {job['name'][:36]:38} waited "
                                    f"{(started - created).total_seconds() / 60:6.1f}m, sibling finished "
                                    f"{gap.total_seconds():5.0f}s before start"
                                )
                        else:
                            no_sibling += 1
                            if len(samples) < 4:
                                shown = f"{gap.total_seconds() / 60:.0f}m" if gap else "none"
                                samples.append(
                                    f"      {job['name'][:36]:38} waited "
                                    f"{(started - created).total_seconds() / 60:6.1f}m, nearest sibling finish {shown}"
                                )

                total = blocked_by_sibling + no_sibling
                if not total:
                    continue
                print(
                    f"{repo:22} {window_name}  stalled={total:4}  "
                    f"started right after a sibling finished: {blocked_by_sibling:4} "
                    f"({blocked_by_sibling / total:.0%})  standalone: {no_sibling:4}"
                )
                for sample in samples:
                    print(sample)
                print(f"      top job names: {', '.join(f'{n} x{c}' for n, c in names.most_common(4))}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
