---
name: "sub-source-audit"
description: "Audit datiya-sub subscription sources: pull latest GitHub Actions build artifacts and attribute surviving nodes per source (raw/dedup/alive counts and protocol mix). Invoke when user asks how many nodes each source provides/retains, which sources to remove, or source quality."
---

# Subscription Source Audit

Audit every Clash subscription source configured in `config.yaml` (`sources:` list):
how many raw nodes each provides, how many survive the full pipeline, and their
protocol composition. Produces a per-source table and flags zero-contribution
sources for removal.

## When to invoke

- User asks "几个源 / 每个源多少节点 / 筛选后剩多少 / 什么类型"
- User asks to remove weak sources or evaluate source quality
- After adding/removing sources, to verify each retained source contributes

## Pipeline context (must follow the real code)

Funnel in `main.py`: 采集 collect (datiya site 7-day articles + extra sources)
→ `builder.dedupe` (JSON key of all fields except `name`; sources merged in
config order, cross-source duplicates credited to the FIRST source)
→ `builder.filter_supported` + `sanitize` → mihomo probes (delay ≤1500ms,
download ≥200KB/s) → `china.filter_reachable` (Globalping CN verdict) →
published `docs/sub/clash.yaml`.

Raw counts are recorded per source in `docs/status.json` (`sources` field) at
build time — prefer that over re-fetching, since upstream sources rotate hourly.

## Steps

1. `git pull --rebase`; read `docs/status.json`: `updated_at`, `sources`
   (raw counts), `alive`, `china_dropped`.
2. Confirm with `gh run list --workflow update.yml --limit 1` that the deployed
   artifact matches the latest successful build. For build funnel numbers
   (采集→去重→清洗→测活→国内可达), download logs:
   `gh api repos/<owner>/<repo>/actions/runs/<id>/logs > logs.zip`, unzip, grep
   `漏斗|附加源合计|strict）：可达`.
3. Attribute final nodes in `docs/sub/clash.yaml` to sources. Node identity
   drifts between re-fetches, so re-fetch each source URL and use the coarse
   fingerprint `(type lowercased, server, port, sni or servername or ws Host)`
   with first-source-wins. Exact full-JSON keys fail because sources rotate
   between the build and the audit. Report unmatched count explicitly.
4. Output table per source: 原始 / 去重后(optional) / 清洗后(optional) /
   最终保留 / 保留节点协议 Counter; plus overall protocol Counter of the
   published subscription and survival rate.
5. Removal recommendation: a source with 0 retained nodes across the LATEST
   build is a candidate; cross-check one prior build if easy, then remove from
   `config.yaml`, commit, push, and verify the triggered build succeeds before
   reporting.

## Conventions

- Dependencies: `pyyaml`, `requests`; run with the workspace python
  (`pip install -q pyyaml requests` if imports fail after environment reset).
- Parse sources only through `builder.parse_proxies` (Clash YAML; has inline
  fallback). Non-Clash formats are not supported by the fetcher.
- gh API is rate-limited unauthenticated (60/h); prefer `gh api`/authenticated
  `gh auth token` headers over raw api.github.com calls.
- Never edit source lists on raw counts alone — retention after the full
  pipeline is the only removal criterion.
