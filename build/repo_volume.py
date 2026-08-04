"""One-off: rank open-telemetry repos by Actions workflow run volume for a date range."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github_actions_analyzer import CachingHttpClient, get_repos  # noqa: E402

START = "2026-07-27T00:00:00Z"
END = "2026-08-01T00:00:00Z"
ORG = "open-telemetry"


def main() -> None:
    client = CachingHttpClient(cache_path=Path("cache.sqlite"))
    try:
        counts = []
        for repo in get_repos(client, ORG):
            uri = (
                f"https://api.github.com/repos/{quote(ORG)}/{quote(repo)}/actions/runs"
                f"?created={quote(f'{START}..{END}')}&per_page=1&exclude_pull_requests=true"
            )
            total = client.get_json(uri).get("total_count", 0)
            if total:
                counts.append((total, repo))
                print(f"{total:>6}  {repo}", flush=True)
    finally:
        client.close()

    print("\n=== ranked ===")
    for total, repo in sorted(counts, reverse=True):
        print(f"{total:>6}  {repo}")
    print(f"\ntotal runs across org: {sum(total for total, _ in counts)}")
    print(f"as of {datetime.now(UTC).isoformat()}")


if __name__ == "__main__":
    main()
