# GitHub Actions queue wait — open-telemetry

Evidence for [open-telemetry/community#3622](https://github.com/open-telemetry/community/issues/3622).
Window: 2026-06-29 → 2026-08-01 (33 days), UTC, GitHub-hosted runners only.

## Problem

Jobs in `opentelemetry-python`, `opentelemetry-python-contrib` and `semantic-conventions` repeatedly wait 30–120 minutes for a GitHub-hosted runner — 35 episodes in 33 days. Meanwhile, on the same `ubuntu-latest` label, other repositories in the same account start jobs in under 5 minutes.

## Same 90 minutes, same label — 2026-07-20 15:30–17:00

| repository | jobs | p50 wait | max wait | >10 min |
|---|---|---|---|---|
| semantic-conventions | 72 | **39.1 min** | [**55.8 min**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29757601458/job/88403894595) | **100%** |
| opentelemetry-js | 109 | **47.9 min** | [**51.9 min**](https://github.com/open-telemetry/opentelemetry-js/actions/runs/29756709375/job/88400799308) | **85%** |
| opentelemetry-go | 127 | 1.3 min | [2.5 min](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29761818449/job/88418025214) | 0% |

`opentelemetry-go` ran *more* hosted jobs than either stalled repository, in the same minutes, and its slowest job waited 2.5 minutes. The full episode lasted **4 hours** and hit four repositories at once.

## Recurrence — 35 episodes in 33 days

Episode = 10-min bucket, ≥5 hosted jobs, p90 wait >10 min, merged within 30 min. Cells: `jobs / p90 min`, where the p90 links to the job that waited that long. **Bold = delayed.** `-` = no hosted jobs in that window.

| episode (UTC) | mins | semantic-conv | python | python-contrib | js | go |
|---|---|---|---|---|---|---|
| 06-30 06:20 | 10 | - | - | 764 / [**27.2**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/28424734975/job/84225193877) | - | - |
| 06-30 15:10 | 10 | - | 1093 / [**13.2**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/28455395698/job/84328675775) | - | - | 83 / [2.5](https://github.com/open-telemetry/opentelemetry-go/actions/runs/28455224308/job/84328040510) |
| 06-30 16:10 | 10 | - | - | 761 / [**14.2**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/28457978316/job/84341049557) | - | - |
| 07-07 17:00 | 10 | 2 / [7.8](https://github.com/open-telemetry/semantic-conventions/actions/runs/28884298717/job/85680375530) | 543 / [**10.7**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/28884273909/job/85680310205) | - | - | 1 / [0.2](https://github.com/open-telemetry/opentelemetry-go/actions/runs/28883676418/job/85679470908) |
| 07-08 14:10 | 10 | - | - | 772 / [**13.2**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/28949648025/job/85892718668) | 41 / [0.4](https://github.com/open-telemetry/opentelemetry-js/actions/runs/28949752419/job/85893006250) | 34 / [1.6](https://github.com/open-telemetry/opentelemetry-go/actions/runs/28949438164/job/85891986934) |
| 07-08 15:20 | 30 | 30 / [**13.2**](https://github.com/open-telemetry/semantic-conventions/actions/runs/28954654603/job/85910264668) | 1087 / [4.7](https://github.com/open-telemetry/opentelemetry-python/actions/runs/28955468261/job/85913142631) | 3032 / [**28.4**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/28955741069/job/85914137538) | - | 30 / [0.8](https://github.com/open-telemetry/opentelemetry-go/actions/runs/28954278425/job/85908954094) |
| 07-09 13:00 | 30 | - | - | 761 / [**25.2**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29020387998/job/86128307487) | - | - |
| 07-09 15:20 | 10 | 16 / [8.4](https://github.com/open-telemetry/semantic-conventions/actions/runs/29029212741/job/86157138891) | 1423 / [**11.1**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29029445832/job/86157982439) | - | - | - |
| 07-09 16:20 | 10 | - | 541 / [**10.6**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29033029964/job/86170584404) | - | - | - |
| 07-10 14:30 | 10 | 14 / [9.7](https://github.com/open-telemetry/semantic-conventions/actions/runs/29100643187/job/86388327600) | 557 / [6.6](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29100332436/job/86387273218) | 756 / [**12.9**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29100397213/job/86387516581) | - | 31 / [0.8](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29100484662/job/86387772726) |
| 07-13 16:40 | 10 | 51 / [8.6](https://github.com/open-telemetry/semantic-conventions/actions/runs/29267679507/job/86877165578) | - | 768 / [**12.1**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29267665493/job/86877194293) | - | 31 / [0.2](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29267509878/job/86876611791) |
| 07-14 13:50 | 20 | - | 1689 / [**27.2**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29339102013/job/87105856418) | - | - | - |
| 07-14 14:50 | 10 | 15 / [**18.5**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29343016825/job/87119430761) | - | - | - | 2 / [0.1](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29342189625/job/87118124942) |
| 07-14 16:30 | 50 | 69 / [**21.2**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29350480555/job/87145130582) | - | - | - | 28 / [2.3](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29353148765/job/87154129662) |
| 07-15 18:10 | 10 | 2 / [0.2](https://github.com/open-telemetry/semantic-conventions/actions/runs/29439443752/job/87434377541) | - | 791 / [**11.3**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29439762578/job/87435471924) | - | - |
| 07-16 14:00 | 40 | 1 / [**44.2**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29506725569/job/87649285418) | 3399 / [**16.9**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29506341952/job/87647973577) | 1581 / [**48.4**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29506299909/job/87647893472) | - | - |
| 07-16 15:30 | 10 | - | 1129 / [**12.6**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29511848516/job/87667018662) | 789 / [5.0](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29511485267/job/87665861582) | - | 30 / [1.2](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29512099212/job/87667854426) |
| 07-20 13:30 | 10 | 1 / [9.7](https://github.com/open-telemetry/semantic-conventions/actions/runs/29746219304/job/88366585546) | 565 / [**12.2**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29746906847/job/88367079797) | - | - | - |
| **07-20 14:30** | **240** | 193 / [**47.2**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29758938453/job/88408390677) | 3348 / [**48.4**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29756331919/job/88399572507) | 792 / [**64.8**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29756423101/job/88399899218) | 269 / [**49.4**](https://github.com/open-telemetry/opentelemetry-js/actions/runs/29756926416/job/88401562337) | 363 / [3.0](https://github.com/open-telemetry/opentelemetry-go/actions/runs/29766105552/job/88432615907) |
| 07-21 12:20 | 50 | 1 / [**24.0**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29830919417/job/88635203313) | 566 / [6.2](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29832619774/job/88640873199) | 2382 / [**26.9**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29830000700/job/88632233424) | 72 / [0.8](https://github.com/open-telemetry/opentelemetry-js/actions/runs/29832504288/job/88640471461) | - |
| 07-21 18:30 | 10 | 13 / [**22.8**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29857931016/job/88726881059) | - | - | - | - |
| 07-22 15:00 | 30 | 17 / [**42.4**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29931696660/job/88962788519) | - | 809 / [**51.0**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29932889176/job/88967001314) | - | - |
| 07-22 16:20 | 70 | 32 / [**17.3**](https://github.com/open-telemetry/semantic-conventions/actions/runs/29937657754/job/88983267732) | 1131 / [**12.6**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/29939773806/job/88990417284) | 795 / [**17.3**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29941952248/job/88997828710) | - | - |
| 07-22 18:30 | 10 | - | - | 791 / [**11.1**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/29946947276/job/89014698762) | - | - |
| 07-24 16:10 | 10 | - | 563 / [**14.5**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30108636072/job/89532246442) | - | - | - |
| 07-27 15:30 | 10 | - | 565 / [**11.8**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30280784035/job/90026372891) | - | - | - |
| 07-27 16:20 | 60 | 139 / [**43.0**](https://github.com/open-telemetry/semantic-conventions/actions/runs/30287889832/job/90050107269) | 1013 / [**15.4**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30286382193/job/90045405896) | - | - | 31 / [4.1](https://github.com/open-telemetry/opentelemetry-go/actions/runs/30286229047/job/90044580436) |
| 07-27 19:00 | 50 | 34 / [**43.8**](https://github.com/open-telemetry/semantic-conventions/actions/runs/30299724572/job/90089403565) | 2269 / [4.8](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30297507980/job/90082051321) | 2373 / [**44.9**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30299554430/job/90088895135) | - | 30 / [2.4](https://github.com/open-telemetry/opentelemetry-go/actions/runs/30296907122/job/90080078640) |
| 07-27 20:30 | 20 | 1 / [**16.1**](https://github.com/open-telemetry/semantic-conventions/actions/runs/30301600060/job/90101114384) | 2 / [0.1](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30302825885/job/90099867051) | 3177 / [**17.5**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30303474523/job/90101908335) | - | 58 / [0.1](https://github.com/open-telemetry/opentelemetry-go/actions/runs/30302849666/job/90099842656) |
| 07-30 14:30 | 10 | 2 / [4.5](https://github.com/open-telemetry/semantic-conventions/actions/runs/30552202087/job/90903436745) | 1130 / [**12.2**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30552233749/job/90903562731) | - | - | - |
| 07-30 19:30 | 30 | - | - | 1570 / [**67.5**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30575233700/job/90981820665) | - | - |
| 07-31 13:50 | 10 | - | - | 774 / [**11.6**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30636368725/job/91174927124) | - | - |
| **07-31 15:20** | 40 | 21 / [**119.2**](https://github.com/open-telemetry/semantic-conventions/actions/runs/30644073007/job/91200970829) | 1697 / [**21.6**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30645084639/job/91204395160) | 775 / [**119.2**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30644038724/job/91200927287) | - | - |
| 07-31 16:40 | 30 | 1 / [**65.9**](https://github.com/open-telemetry/semantic-conventions/actions/runs/30643194508/job/91214311796) | 1730 / [**14.8**](https://github.com/open-telemetry/opentelemetry-python/actions/runs/30648487913/job/91215731315) | 1544 / [**66.1**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30648091901/job/91214482627) | - | - |
| 07-31 20:20 | 10 | - | - | 1545 / [**15.5**](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30662965480/job/91263310753) | - | 32 / [1.4](https://github.com/open-telemetry/opentelemetry-go/actions/runs/30663064545/job/91263581752) |

| repository | stalled in | had jobs running in |
|---|---|---|
| opentelemetry-python-contrib | **21 / 35** | 22 |
| opentelemetry-python | **16 / 35** | 21 |
| semantic-conventions | **14 / 35** | 21 |
| opentelemetry-js | 1 / 35 | 3 |
| opentelemetry-go | 0 / 35 | 14 |

On 07-31 15:20, [`semantic-conventions`](https://github.com/open-telemetry/semantic-conventions/actions/runs/30644073007/job/91200970829) and [`opentelemetry-python-contrib`](https://github.com/open-telemetry/opentelemetry-python-contrib/actions/runs/30644038724/job/91200927287) both reach p90 **119.2 min** — two independent repositories at an identical wait.

## Not caused by us

| hypothesis | evidence against |
|---|---|
| Self-hosted pools | excluded from all figures; hosted labels only |
| Our burst size / matrix width | inverted — go's 371-job burst → p90 1.5 min; semantic-conventions 1–3 job buckets → p90 119 min |
| Fork-PR approval latency | js non-fork PRs 9% >10m vs fork 1%; `merge_group` and `push` also affected; go runs 9,612 fork-PR jobs at 0% |
| `needs:` wait counted as queue time | stalled jobs are entry-point jobs — [`get-changed-files` waited 119.2 min](https://github.com/open-telemetry/semantic-conventions/actions/runs/30644073007/job/91200970829) with zero sibling completions |
| Runner label differences | all comparisons are `ubuntu-latest` |

Delay is event-agnostic (`push`, `merge_group`, `pull_request`, `schedule`) and does not track our volume — our busiest periods are often our fastest.

## Open question

Repo-selective, with no configuration difference found to explain it. Leading untested hypothesis: a concurrency ceiling **above** the organisation — the CNCF enterprise account is shared across all CNCF orgs — plus uneven allocation between competing repositories.

## Asks

**GitHub**
1. On 2026-07-20 14:30–18:30 UTC, why did `ubuntu-latest` jobs in `semantic-conventions` and `opentelemetry-js` queue 40–55 min while `opentelemetry-go` — more jobs, same account, same minutes — started in 3 min?
2. Is a concurrency limit being hit, and at what scope (repo / org / enterprise)?
3. How is capacity allocated between repositories competing for it?
4. Can we get queued-job metrics to monitor this directly?

**CNCF**
1. What is the enterprise concurrent-job entitlement, and is it shared across all CNCF orgs?
2. What headroom remains, and can it be raised?
3. Are larger or dedicated runners an option for open-telemetry?

## Method

Per **job**, `started_at - created_at` from the Actions Jobs API. Excludes jobs that never got a runner (they report `started_at == created_at`), retries, and non-hosted labels. Full table: `python build/incidents.py`.

```bash
export GITHUB_AUTH_TOKEN=$(gh auth token)
python github_actions_analyzer.py --repo semantic-conventions \
  --start 2026-07-20T15:30:00Z --end 2026-07-20T17:00:00Z \
  --group-by label --min-queue-minutes 0
```

## Caveats

- **No clean baseline for the python repos** — 06-30 already shows 7% over 10 min, so within this window the problem is chronic rather than demonstrably a recent regression.
- `opentelemetry-go` was picked *after* observing it was unaffected. It proves hosted capacity was available at those minutes, not that it is immune.
- `opentelemetry-js` had jobs in only 3 of 35 windows; its low count is absence of data.
- 20 runs (<0.5%) skipped — jobs endpoint returns HTTP 502 permanently.
- Episodic, not a daily pattern. Most days are clean; bad days fall in 15:00–20:00 UTC (10:00–15:00 US Central).
