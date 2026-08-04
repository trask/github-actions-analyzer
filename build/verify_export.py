"""Check the export against the published comment and against the GitHub API,
so the attached files can be trusted without re-running the collection."""

import csv
import random
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

OUT = Path("build/export")
COMMENT = "repos/open-telemetry/community/issues/comments/5171791363"
SHORT = {
    "semantic-conventions": "semantic-conv",
    "opentelemetry-python": "python",
    "opentelemetry-python-contrib": "python-contrib",
    "opentelemetry-js": "js",
    "opentelemetry-go": "go",
}

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"{'ok  ' if condition else 'FAIL'} {message}")
    if not condition:
        failures.append(message)


def _rows(name: str) -> list[dict[str, str]]:
    with (OUT / name).open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _instant(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")


def gate_published_table(episodes: list[dict[str, str]]) -> None:
    body = subprocess.run(
        ["gh", "api", COMMENT, "--jq", ".body"],
        capture_output=True, check=True, encoding="utf-8",
    ).stdout

    published: dict[tuple[str, str], str] = {}
    order = [SHORT[repo] for repo in SHORT]
    for line in body.splitlines():
        match = re.match(r"^\| \*{0,2}(\d\d-\d\d \d\d:\d\d)\*{0,2} \|", line)
        if not match:
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        for name, cell in zip(order, cells[2:]):
            if cell != "-":
                jobs, p90 = cell.split(" / ")
                published[(match.group(1), name)] = f"{jobs} {re.sub(r'[*]', '', p90.split(']')[0][1:])}"

    derived = {
        (_instant(row["episode_start_utc"]).strftime("%m-%d %H:%M"), SHORT[row["repo"]]):
            f"{row['jobs']} {float(row['p90_min']):.1f}"
        for row in episodes
    }
    check(derived == published, f"episodes.csv reproduces the published table ({len(published)} cells)")
    for key in sorted(set(derived) | set(published)):
        if derived.get(key) != published.get(key):
            print(f"     {key}: export={derived.get(key)!r} published={published.get(key)!r}")


def gate_job_invariants(jobs: list[dict[str, str]]) -> None:
    check(all(row["runner_name"] for row in jobs), "every job has a runner_name (never-started jobs excluded)")
    check(
        all(
            abs(
                (_instant(row["started_at"]) - _instant(row["created_at"])).total_seconds() / 60
                - float(row["wait_minutes"])
            )
            < 0.01
            for row in jobs
        ),
        "wait_minutes equals started_at - created_at",
    )
    waits = [float(row["wait_minutes"]) for row in jobs]
    check(all(0 <= wait <= 360 for wait in waits), f"all waits within 0-6h (max {max(waits):.1f} min)")
    check(all(row["labels"].startswith(("ubuntu-", "windows-", "macos-")) for row in jobs), "hosted labels only")


def gate_totals(episodes: list[dict[str, str]], jobs: list[dict[str, str]]) -> None:
    counted: dict[tuple[str, str], int] = {}
    for row in jobs:
        key = (row["episode_start_utc"], row["repo"])
        counted[key] = counted.get(key, 0) + 1
    declared = {(row["episode_start_utc"], row["repo"]): int(row["jobs"]) for row in episodes}
    check(counted == declared, f"episode-jobs.csv row counts match the jobs column ({sum(declared.values())} jobs)")


def gate_hourly(episodes: list[dict[str, str]], hourly: list[dict[str, str]]) -> None:
    stalled_hours = {
        (_instant(row["episode_start_utc"]).replace(minute=0), row["repo"])
        for row in episodes
        if row["stalled"] == "True"
    }
    quiet = [
        row
        for row in hourly
        if (_instant(row["hour_utc"]), row["repo"]) not in stalled_hours and int(row["jobs"]) >= 50
    ]
    calm = sum(1 for row in quiet if float(row["p90_min"]) <= 10)
    check(len(quiet) > 500, f"hourly.csv covers the whole window, not only the bad hours ({len(hourly)} rows)")
    print(f"     outside stalled hours: {calm}/{len(quiet)} busy hours had p90 <= 10 min")


def gate_spot_check(jobs: list[dict[str, str]]) -> None:
    for row in random.Random(0).sample(jobs, 3):
        result = subprocess.run(
            [
                "gh", "api", f"repos/open-telemetry/{row['repo']}/actions/jobs/{row['job_id']}",
                "--jq", "[.created_at, .started_at, .runner_name] | @tsv",
            ],
            capture_output=True, encoding="utf-8",
        )
        if result.returncode != 0:
            check(False, f"job {row['job_id']} fetch failed: {result.stderr.strip()[:120]}")
            continue
        created, started, runner = result.stdout.strip().split("\t")
        wait = (_instant(started) - _instant(created)).total_seconds() / 60
        check(
            created == row["created_at"] and started == row["started_at"] and runner == row["runner_name"],
            f"job {row['job_id']} matches live API (wait {wait:.2f} vs {row['wait_minutes']})",
        )


def main() -> None:
    episodes = _rows("episodes.csv")
    jobs = _rows("episode-jobs.csv")
    hourly = _rows("hourly.csv")

    gate_published_table(episodes)
    gate_job_invariants(jobs)
    gate_totals(episodes, jobs)
    gate_hourly(episodes, hourly)
    gate_spot_check(jobs)

    size = Path("build/queue-wait-raw-data.zip").stat().st_size
    check(size < 25_000_000, f"zip is {size / 1_000_000:.1f} MB (GitHub limit 25 MB)")

    print(f"\n{len(failures)} failures")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
