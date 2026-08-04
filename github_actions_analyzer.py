from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from http.client import HTTPException, HTTPResponse
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen


START = datetime(2026, 7, 14, hour=15, tzinfo=UTC)
END = datetime(2026, 7, 14, hour=17, tzinfo=UTC)
PAGE_SIZE = 100
MIN_QUEUE_TIME = timedelta(minutes=10)
WORKFLOW_TIMEOUT = timedelta(hours=6)
SECONDARY_LIMIT_BACKOFF = 10
MAX_RETRIES = 5
SERVER_ERROR_BACKOFF = 2
SERVER_ERROR_RETRIES = 2
REQUEST_TIMEOUT = 30
HOSTED_LABEL_PREFIXES = ("ubuntu-", "windows-", "macos-")
ORG = "open-telemetry"


class ServerError(RuntimeError):
    """A 5xx that persists across retries; some runs return 502 for their jobs forever."""


class CachingHttpClient:
    def __init__(
        self,
        cache_path: Path = Path("cache.sqlite"),
        print_progress: bool = False,
    ) -> None:
        self._auth_token = os.getenv("GITHUB_AUTH_TOKEN")
        self._print_progress = print_progress
        self._connection = sqlite3.connect(cache_path)
        self._connection.execute(
            "create table if not exists cache (uri text primary key, body text not null, etag text)"
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def get_json(self, uri: str, immutable: bool = False) -> Any:
        return json.loads(self._get(uri, immutable))

    def _get(self, uri: str, immutable: bool = False, attempt: int = 0) -> str:
        row = self._connection.execute(
            "select body, etag from cache where uri = ?", (uri,)
        ).fetchone()
        cached_body = row[0] if row else None
        cached_etag = row[1] if row else None

        if cached_body is not None and cached_etag is not None:
            if (
                immutable
                or self._is_completed_historical_runs_response(uri, cached_body)
            ):
                self._print("immutable cache", uri)
                return cached_body

            response = self._send(uri, cached_etag)
            if isinstance(response, HTTPError) and response.code == 304:
                self._print("found in cache", uri, response)
                return cached_body
            if isinstance(response, HTTPError):
                return self._retry_if_possible(response, uri, immutable, attempt)

            body = response.read().decode("utf-8")
            etag = response.headers.get("ETag")
            if etag is not None:
                self._connection.execute(
                    "update cache set body = ?, etag = ? where uri = ?", (body, etag, uri)
                )
                self._connection.commit()
            self._print("found in cache, but since updated", uri, response)
            return body

        response = self._send(uri)
        if isinstance(response, HTTPError):
            return self._retry_if_possible(response, uri, immutable, attempt)

        body = response.read().decode("utf-8")
        etag = response.headers.get("ETag")
        if etag is not None:
            self._connection.execute(
                "insert into cache (uri, body, etag) values (?, ?, ?)", (uri, body, etag)
            )
            self._connection.commit()
            self._print("stored in cache", uri, response)
        else:
            self._print("no etag", uri, response)
        return body

    @staticmethod
    def _is_completed_historical_runs_response(uri: str, body: str) -> bool:
        parsed_uri = urlparse(uri)
        if not parsed_uri.path.endswith("/actions/runs"):
            return False

        created_ranges = parse_qs(parsed_uri.query).get("created")
        if not created_ranges:
            return False

        _, separator, end = created_ranges[0].partition("..")
        if not separator or _parse_instant(end) > datetime.now(UTC):
            return False

        workflow_runs = json.loads(body).get("workflow_runs", [])
        return all(run.get("status") == "completed" for run in workflow_runs)

    def _send(self, uri: str, etag: str | None = None) -> HTTPResponse | HTTPError:
        headers = {"Accept": "application/vnd.github+json"}
        if self._auth_token:
            headers["Authorization"] = f"token {self._auth_token}"
        if etag is not None:
            headers["If-None-Match"] = etag

        request = Request(uri, headers=headers, method="GET")
        for attempt in range(SERVER_ERROR_RETRIES + 1):
            try:
                return urlopen(request, timeout=REQUEST_TIMEOUT)
            except HTTPError as e:
                return e
            # dropped connections and timeouts are not HTTPErrors, so retry them here
            except (HTTPException, OSError) as e:
                if attempt == SERVER_ERROR_RETRIES:
                    raise
                print(f"{type(e).__name__}, retrying: {uri}")
                time.sleep(SERVER_ERROR_BACKOFF * 2**attempt)
        raise AssertionError("unreachable")

    def _retry_if_possible(
        self,
        response: HTTPError,
        uri: str,
        immutable: bool = False,
        attempt: int = 0,
    ) -> str:
        retry_after = response.headers.get("retry-after")
        if retry_after is not None:
            sleep_seconds = int(retry_after)
            print(f"retry-after, sleeping for {sleep_seconds} seconds: {uri}")
            time.sleep(sleep_seconds)
            return self._get(uri, immutable, attempt + 1)

        if response.code >= 500:
            if attempt >= SERVER_ERROR_RETRIES:
                raise ServerError(f"Server error {response.code} after {attempt} retries: {uri}")
            time.sleep(SERVER_ERROR_BACKOFF * 2**attempt)
            return self._get(uri, immutable, attempt + 1)

        ratelimit_reset = response.headers.get("x-ratelimit-reset")
        if ratelimit_reset is not None:
            # budget left means this is a secondary (burst) limit, which clears in seconds
            if int(response.headers.get("x-ratelimit-remaining") or 0) > 0:
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(
                        f"Gave up after {attempt} retries, response {response.code}: {uri}\n"
                        f"{response.headers}\n{response.read().decode('utf-8')}"
                    )
                sleep_seconds = SECONDARY_LIMIT_BACKOFF * 2**attempt
                print(f"secondary rate limit, sleeping for {sleep_seconds} seconds: {uri}")
                time.sleep(sleep_seconds)
                return self._get(uri, immutable, attempt + 1)

            reset_epoch_seconds = int(ratelimit_reset)
            sleep_seconds = max(0, reset_epoch_seconds - int(time.time()))
            reset_time = datetime.fromtimestamp(reset_epoch_seconds).time()
            print(f"x-ratelimit-reset, sleeping until {reset_time}")
            time.sleep(sleep_seconds)
            return self._get(uri, immutable, attempt + 1)

        retry_at = response.headers.get("date")
        retry_at_text = ""
        if retry_at is not None:
            retry_at_text = f" at {parsedate_to_datetime(retry_at)}"
        body = response.read().decode("utf-8")
        raise RuntimeError(
            f"Unexpected response {response.code}{retry_at_text}: {uri}\n{response.headers}\n{body}"
        )

    def _print(self, message: str, uri: str, response: HTTPResponse | HTTPError | None = None) -> None:
        if not self._print_progress:
            return
        remaining = "n/a" if response is None else response.headers.get("x-ratelimit-remaining", "n/a")
        print(f"{message}: {uri}, x-ratelimit-remaining: {remaining}")


@dataclass(frozen=True)
class AnalyzerConfig:
    org: str
    start: datetime
    end: datetime
    min_queue_time: timedelta
    workflow_timeout: timedelta
    tz_offset: timedelta


@dataclass(frozen=True)
class JobRecord:
    repo: str
    run_id: int
    job_id: int
    workflow: str
    name: str
    created_at: datetime
    queue_duration: timedelta
    labels: str
    event: str
    from_fork: bool
    runner_name: str
    html_url: str


def main() -> None:
    args = _parse_args()
    config = AnalyzerConfig(
        org=args.org,
        start=args.start,
        end=args.end,
        min_queue_time=timedelta(minutes=args.min_queue_minutes),
        workflow_timeout=timedelta(hours=args.workflow_timeout_hours),
        tz_offset=timedelta(hours=args.tz_offset_hours),
    )

    client = CachingHttpClient(
        cache_path=args.cache_path,
        print_progress=args.print_progress,
    )
    try:
        repos = args.repo if args.repo else get_repos(client, config.org)
        records: list[JobRecord] = []
        for repo in repos:
            if args.group_by == "job":
                print(repo)
            records.extend(get_workflow_data(client, config, repo, print_jobs=args.group_by == "job"))
    finally:
        client.close()

    if args.label:
        records = [record for record in records if record.labels in args.label]
    else:
        records = [record for record in records if _is_hosted(record.labels)]

    if args.event:
        records = [record for record in records if record.event in args.event]
    if args.exclude_forks:
        records = [record for record in records if not record.from_fork]

    if args.group_by == "hour":
        _print_grouped(records, lambda r: f"{(r.created_at + config.tz_offset).hour:02d}", "hour")
    elif args.group_by == "day":
        _print_grouped(records, lambda r: (r.created_at + config.tz_offset).date().isoformat(), "day")
    elif args.group_by == "label":
        _print_grouped(records, lambda r: r.labels, "label")
    elif args.group_by == "event":
        _print_grouped(records, lambda r: f"{r.event}{' (fork)' if r.from_fork else ''}", "event")


def get_workflow_data(
    client: CachingHttpClient,
    config: AnalyzerConfig,
    repo: str,
    print_jobs: bool = True,
) -> list[JobRecord]:
    records: list[JobRecord] = []
    for workflow_run in get_workflow_runs(client, config, repo):
        if workflow_run["run_attempt"] > 1:
            continue

        try:
            jobs = get_jobs(
                client,
                config.org,
                repo,
                workflow_run["id"],
                immutable=workflow_run.get("status") == "completed",
            )
        except ServerError as e:
            print(f"WARNING: skipping run, {e}")
            continue

        head_repository = workflow_run.get("head_repository") or {}
        from_fork = head_repository.get("full_name") != f"{config.org}/{repo}"

        for job in jobs:
            # jobs that never got a runner report started_at == created_at, which hides the wait
            if job.get("started_at") is None or not job.get("runner_name"):
                continue

            queue_duration = _duration(job["created_at"], job["started_at"])
            if not timedelta() <= queue_duration <= config.workflow_timeout:
                continue

            records.append(
                JobRecord(
                    repo=repo,
                    run_id=workflow_run["id"],
                    job_id=job["id"],
                    workflow=workflow_run["name"],
                    name=job["name"],
                    created_at=_parse_instant(job["created_at"]),
                    queue_duration=queue_duration,
                    labels=",".join(job.get("labels") or ["(none)"]),
                    event=workflow_run["event"],
                    from_fork=from_fork,
                    runner_name=job["runner_name"],
                    html_url=job["html_url"],
                )
            )

            if print_jobs and queue_duration > config.min_queue_time:
                print(
                    f"{job['created_at']} -- "
                    f"{math.floor(queue_duration.total_seconds() / 60)} -- "
                    f"{job['html_url']}"
                )
    return records


def _print_grouped(
    records: list[JobRecord],
    key: Any,
    label: str,
) -> None:
    groups: dict[str, list[JobRecord]] = defaultdict(list)
    for record in records:
        groups[key(record)].append(record)

    print(
        f"{label:<28}  {'jobs':>6}  {'p50':>6}  {'p90':>6}  {'p99':>6}  {'max':>6}  {'>10m':>5}  "
        f"{'slowest job'}"
    )
    for group in sorted(groups):
        ordered = sorted(groups[group], key=lambda record: record.queue_duration)
        minutes = [record.queue_duration.total_seconds() / 60 for record in ordered]
        over_ten = sum(1 for value in minutes if value > 10)
        print(
            f"{group:<28}  {len(minutes):>6}  "
            f"{_percentile(minutes, 50):>6.1f}  {_percentile(minutes, 90):>6.1f}  "
            f"{_percentile(minutes, 99):>6.1f}  {minutes[-1]:>6.1f}  "
            f"{100 * over_ten / len(minutes):>4.0f}%  "
            f"{ordered[-1].html_url}"
        )


def _percentile(sorted_minutes: list[float], percentile: float) -> float:
    index = math.ceil(percentile / 100 * len(sorted_minutes)) - 1
    return sorted_minutes[min(max(index, 0), len(sorted_minutes) - 1)]


# donated self-hosted pools queue for hours and would swamp GitHub-hosted numbers
def _is_hosted(labels: str) -> bool:
    return all(label.startswith(HOSTED_LABEL_PREFIXES) for label in labels.split(","))


def get_repos(client: CachingHttpClient, org: str) -> list[str]:
    repos = []
    page = 1
    while True:
        response = client.get_json(
            f"https://api.github.com/orgs/{quote(org)}/repos?per_page={PAGE_SIZE}&page={page}"
        )
        if not response:
            return sorted(repo["name"] for repo in repos)
        repos.extend(response)
        page += 1


def get_workflow_runs(
    client: CachingHttpClient,
    config: AnalyzerConfig,
    repo: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict[str, Any]]:
    start = config.start if start is None else start
    end = config.end if end is None else end

    runs_response = get_runs_response(client, config.org, repo, start, end, 1)

    if runs_response["total_count"] > 1000:
        results: list[dict[str, Any]] = []
        for split_start, split_end in _split_range(start, end):
            results.extend(get_workflow_runs(client, config, repo, split_start, split_end))
        return results

    results = list(runs_response["workflow_runs"])
    total_pages = math.ceil(runs_response["total_count"] / PAGE_SIZE)
    for page in range(2, total_pages + 1):
        results.extend(get_runs_response(client, config.org, repo, start, end, page)["workflow_runs"])

    # runs created while paging shift later pages, so dedupe and tolerate a small shortfall
    deduped = list({run["id"]: run for run in results}.values())
    expected = runs_response["total_count"]
    if len(deduped) != expected:
        print(f"WARNING: {repo} {_format_instant(start)}..{_format_instant(end)} "
              f"returned {len(deduped)} of {expected} runs")

    deduped.sort(key=lambda run: _parse_instant(run["created_at"]))
    return deduped


def get_runs_response(
    client: CachingHttpClient,
    org: str,
    repo: str,
    start: datetime,
    end: datetime,
    page: int,
) -> dict[str, Any]:
    created_range = f"{_format_instant(start)}..{_format_instant(end)}"
    path = (
        f"/actions/runs?created={quote(created_range)}"
        f"&per_page={PAGE_SIZE}&page={page}&exclude_pull_requests=true"
    )
    return send_repo_request(client, org, repo, path)


def get_jobs(
    client: CachingHttpClient,
    org: str,
    repo: str,
    workflow_run_id: int,
    immutable: bool = False,
) -> list[dict[str, Any]]:
    jobs_response = get_jobs_response(client, org, repo, workflow_run_id, 1, immutable)

    results = list(jobs_response["jobs"])
    total_pages = math.ceil(jobs_response["total_count"] / PAGE_SIZE)
    for page in range(2, total_pages + 1):
        results.extend(get_jobs_response(client, org, repo, workflow_run_id, page, immutable)["jobs"])

    if len(results) != jobs_response["total_count"]:
        raise RuntimeError(
            "INCORRECT NUMBER OF RESULTS\n"
            f"{len(results)}\n"
            f"{jobs_response['total_count']}"
        )
    return results


def get_jobs_response(
    client: CachingHttpClient,
    org: str,
    repo: str,
    workflow_run_id: int,
    page: int,
    immutable: bool = False,
) -> dict[str, Any]:
    path = f"/actions/runs/{workflow_run_id}/jobs?per_page={PAGE_SIZE}&page={page}"
    return send_repo_request(client, org, repo, path, immutable)


def send_repo_request(
    client: CachingHttpClient,
    org: str,
    repo: str,
    path: str,
    immutable: bool = False,
) -> dict[str, Any]:
    return client.get_json(
        f"https://api.github.com/repos/{quote(org)}/{quote(repo)}{path}",
        immutable,
    )


def _split_range(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    difference = end - start
    if difference > timedelta(days=1):
        interval = timedelta(days=1)
    elif difference > timedelta(hours=1):
        interval = timedelta(hours=1)
    elif difference > timedelta(minutes=1):
        interval = timedelta(minutes=1)
    else:
        interval = timedelta(seconds=1)

    split_start = start
    split_end = start + interval
    while split_end <= end:
        yield split_start, split_end
        split_start = split_end
        split_end += interval


def _duration(start: str, end: str) -> timedelta:
    return _parse_instant(end) - _parse_instant(start)


def _parse_instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _format_instant(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze GitHub Actions queue times.")
    parser.add_argument("--org", default=ORG)
    parser.add_argument("--repo", action="append")
    parser.add_argument("--start", type=_parse_instant, default=START)
    parser.add_argument("--end", type=_parse_instant, default=END)
    parser.add_argument("--group-by", choices=("job", "hour", "day", "label", "event"), default="job")
    parser.add_argument(
        "--label",
        action="append",
        help="restrict to these runner labels; defaults to all GitHub-hosted labels",
    )
    parser.add_argument(
        "--event",
        action="append",
        help="restrict to these trigger events; defaults to all",
    )
    parser.add_argument(
        "--exclude-forks",
        action="store_true",
        help="drop runs from forks, whose wait includes maintainer approval latency",
    )
    parser.add_argument("--tz-offset-hours", type=int, default=0)
    parser.add_argument("--min-queue-minutes", type=int, default=int(MIN_QUEUE_TIME.total_seconds() / 60))
    parser.add_argument("--workflow-timeout-hours", type=int, default=int(WORKFLOW_TIMEOUT.total_seconds() / 3600))
    parser.add_argument("--cache-path", type=Path, default=Path("cache.sqlite"))
    parser.add_argument("--print-progress", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    main()