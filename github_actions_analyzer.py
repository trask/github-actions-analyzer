from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from http.client import HTTPResponse
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


START = datetime(2026, 5, 19, hour=13, tzinfo=UTC)
END = datetime(2026, 5, 19, hour=17, tzinfo=UTC)
PAGE_SIZE = 100
MIN_QUEUE_TIME = timedelta(minutes=10)
WORKFLOW_TIMEOUT = timedelta(hours=6)
ORG = "open-telemetry"


class CachingHttpClient:
    def __init__(
        self,
        cache_path: Path = Path("cache.sqlite"),
        bypass_etag_check: bool = False,
        print_progress: bool = False,
    ) -> None:
        self._auth_token = os.getenv("GITHUB_AUTH_TOKEN")
        self._bypass_etag_check = bypass_etag_check
        self._print_progress = print_progress
        self._connection = sqlite3.connect(cache_path)
        self._connection.execute(
            "create table if not exists cache (uri text primary key, body text not null, etag text)"
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def get_json(self, uri: str) -> Any:
        return json.loads(self._get(uri))

    def _get(self, uri: str) -> str:
        row = self._connection.execute(
            "select body, etag from cache where uri = ?", (uri,)
        ).fetchone()
        cached_body = row[0] if row else None
        cached_etag = row[1] if row else None

        if cached_body is not None and cached_etag is not None:
            if self._bypass_etag_check:
                self._print("bypass etag check", uri)
                return cached_body

            response = self._send(uri, cached_etag)
            if isinstance(response, HTTPError) and response.code == 304:
                self._print("found in cache", uri, response)
                return cached_body
            if isinstance(response, HTTPError):
                return self._retry_if_possible(response, uri)

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
            return self._retry_if_possible(response, uri)

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

    def _send(self, uri: str, etag: str | None = None) -> HTTPResponse | HTTPError:
        headers = {"Accept": "application/vnd.github+json"}
        if self._auth_token:
            headers["Authorization"] = f"token {self._auth_token}"
        if etag is not None:
            headers["If-None-Match"] = etag

        request = Request(uri, headers=headers, method="GET")
        try:
            return urlopen(request)
        except HTTPError as e:
            return e

    def _retry_if_possible(self, response: HTTPError, uri: str) -> str:
        retry_after = response.headers.get("retry-after")
        if retry_after is not None:
            sleep_seconds = int(retry_after)
            print(f"retry-after, sleeping for {sleep_seconds} seconds")
            time.sleep(sleep_seconds)
            return self._get(uri)

        ratelimit_reset = response.headers.get("x-ratelimit-reset")
        if ratelimit_reset is not None:
            reset_epoch_seconds = int(ratelimit_reset)
            sleep_seconds = max(0, reset_epoch_seconds - int(time.time()))
            reset_time = datetime.fromtimestamp(reset_epoch_seconds).time()
            print(f"x-ratelimit-reset, sleeping until {reset_time}")
            time.sleep(sleep_seconds)
            return self._get(uri)

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


def main() -> None:
    args = _parse_args()
    config = AnalyzerConfig(
        org=args.org,
        start=args.start,
        end=args.end,
        min_queue_time=timedelta(minutes=args.min_queue_minutes),
        workflow_timeout=timedelta(hours=args.workflow_timeout_hours),
    )

    client = CachingHttpClient(
        cache_path=args.cache_path,
        bypass_etag_check=args.bypass_etag_check,
        print_progress=args.print_progress,
    )
    try:
        for repo in get_repos(client, config.org):
            print(repo)
            get_workflow_data(client, config, repo)
    finally:
        client.close()


def get_workflow_data(client: CachingHttpClient, config: AnalyzerConfig, repo: str) -> None:
    for workflow_run in get_workflow_runs(client, config, repo):
        if workflow_run["run_attempt"] > 1:
            continue

        duration = _duration(workflow_run["created_at"], workflow_run["updated_at"])
        if duration < config.min_queue_time or duration > config.workflow_timeout:
            continue

        alt_duration_minutes = duration.total_seconds() / 60
        if alt_duration_minutes <= 60 or alt_duration_minutes >= 360:
            continue

        jobs = get_jobs(client, config.org, repo, workflow_run["id"])
        first_job_started_at = min(
            (_parse_instant(job["started_at"]) for job in jobs if job.get("started_at") is not None),
            default=None,
        )
        if first_job_started_at is None:
            queue_duration = duration
        else:
            queue_duration = first_job_started_at - _parse_instant(workflow_run["created_at"])

        if queue_duration > config.min_queue_time:
            print(
                f"{workflow_run['created_at']} -- "
                f"{math.floor(queue_duration.total_seconds() / 60)} -- "
                f"{math.floor(duration.total_seconds() / 60)} -- "
                f"{workflow_run['html_url']}"
            )


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

    if len(results) != runs_response["total_count"]:
        raise RuntimeError(
            "INCORRECT NUMBER OF RESULTS\n"
            f"{len(results)}\n"
            f"{runs_response['total_count']}"
        )

    results.sort(key=lambda run: _parse_instant(run["created_at"]))
    return results


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
        f"&event=pull_request&per_page={PAGE_SIZE}&page={page}&exclude_pull_requests=true"
    )
    return send_repo_request(client, org, repo, path)


def get_jobs(client: CachingHttpClient, org: str, repo: str, workflow_run_id: int) -> list[dict[str, Any]]:
    jobs_response = get_jobs_response(client, org, repo, workflow_run_id, 1)

    results = list(jobs_response["jobs"])
    total_pages = math.ceil(jobs_response["total_count"] / PAGE_SIZE)
    for page in range(2, total_pages + 1):
        results.extend(get_jobs_response(client, org, repo, workflow_run_id, page)["jobs"])

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
) -> dict[str, Any]:
    path = f"/actions/runs/{workflow_run_id}/jobs?per_page={PAGE_SIZE}&page={page}"
    return send_repo_request(client, org, repo, path)


def send_repo_request(client: CachingHttpClient, org: str, repo: str, path: str) -> dict[str, Any]:
    return client.get_json(f"https://api.github.com/repos/{quote(org)}/{quote(repo)}{path}")


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
    parser.add_argument("--start", type=_parse_instant, default=START)
    parser.add_argument("--end", type=_parse_instant, default=END)
    parser.add_argument("--min-queue-minutes", type=int, default=int(MIN_QUEUE_TIME.total_seconds() / 60))
    parser.add_argument("--workflow-timeout-hours", type=int, default=int(WORKFLOW_TIMEOUT.total_seconds() / 3600))
    parser.add_argument("--cache-path", type=Path, default=Path("cache.sqlite"))
    parser.add_argument("--bypass-etag-check", action="store_true")
    parser.add_argument("--print-progress", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    main()