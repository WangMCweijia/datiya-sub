"""free.datiya.com 免费节点聚合：采集 -> 测活 -> 合并 -> 输出固定订阅。"""

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

import builder
import checker
import china
import history
import prober
import whitelist
from fetcher import Fetcher

log = logging.getLogger("datiya")

ROOT = Path(__file__).resolve().parent


def setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def load_config(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def collect(fetcher, days):
    """采集多天节点，返回 (节点列表, 成功的日期列表)。"""
    proxies, ok_days = [], []
    for day in days:
        text = fetcher.fetch_day(day)
        if not text:
            log.warning("跳过 %s：未取到配置", day)
            continue
        parsed = builder.parse_proxies(text)
        log.info("%s：解析到 %d 个节点", day, len(parsed))
        proxies.extend(parsed)
        ok_days.append(day)
    return proxies, ok_days


def collect_extra(fetcher, sources):
    """采集附加订阅源，返回 (节点列表, {源名: 节点数})。"""
    proxies, stats = [], {}
    for src in sources or []:
        name = src.get("name") or src.get("url", "?")
        text = fetcher.fetch_text(src["url"])
        if not text:
            log.warning("附加源 %s：未取到内容，跳过", name)
            stats[name] = 0
            continue
        parsed = builder.parse_proxies(text)
        log.info("附加源 %s：解析到 %d 个节点", name, len(parsed))
        stats[name] = len(parsed)
        proxies.extend(parsed)
    return proxies, stats


def render_page(status, out_cfg):
    rows = "".join(
        f"<tr><td>{label}</td><td><code>{path}</code></td></tr>"
        for label, path in (
            ("Clash / Mihomo", out_cfg["clash_file"]),
            ("v2rayN / v2rayNG", out_cfg["v2ray_file"]),
        )
    )
    regions = "、".join(f"{k} {v}" for k, v in status["regions"].items()) or "无"
    sources = status.get("sources") or {}
    source_row = ""
    if sources:
        detail = "、".join(f"{name} {cnt}" for name, cnt in sources.items())
        source_row = f"<tr><th>附加源</th><td>{len(sources)} 个（{detail}）</td></tr>"
    china_row = ""
    if status.get("china_check"):
        china_row = (
            f"<tr><th>国内不可达剔除</th><td>{status.get('china_dropped', 0)} 个"
            f"（Globalping 中国大陆探测）</td></tr>"
        )
    wl_row = ""
    wl = status.get("whitelist") or {}
    if wl.get("used"):
        rejected = wl.get("rejected") or {}
        wl_row = (
            f"<tr><th>国内实测</th><td>{wl.get('probe_host') or '本机'}"
            f"真实代理测速白名单，{wl.get('age_hours') or 0:.1f} 小时前生成；"
            f"剔除 失效 {rejected.get('dead', 0)} · 不稳定 {rejected.get('unstable', 0)}"
            f" · 过慢 {rejected.get('slow', 0)}</td></tr>"
        )
    proto_row = ""
    if status.get("protocols"):
        detail = "、".join(f"{kind} {count}" for kind, count in status["protocols"].items())
        proto_row = f"<tr><th>协议构成</th><td>{detail}</td></tr>"
    line_row = ""
    if status.get("unique_lines"):
        line_row = (
            f"<tr><th>独立线路</th><td>{status['unique_lines']} 条"
            f"（唯一率 {status.get('line_uniqueness', 0):.0%}，越低说明重复越多）</td></tr>"
        )
    streak_row = ""
    streaks = status.get("streak") or {}
    if streaks:
        streak_row = (
            f"<tr><th>存活轮次</th><td>本轮新出现 {streaks.get('first', 0)} 个 · "
            f"连续 2 轮 {streaks.get('repeat', 0)} 个 · "
            f"连续 3 轮以上 {streaks.get('trusted', 0)} 个（置顶优先）</td></tr>"
        )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>固定订阅 - 免费节点聚合</title>
<style>
  body {{ font-family: system-ui, -apple-system, "Segoe UI", sans-serif; max-width: 720px;
         margin: 40px auto; padding: 0 16px; line-height: 1.7; color: #1a1a1a; }}
  h1 {{ font-size: 1.4rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
  td, th {{ border-bottom: 1px solid #e5e5e5; padding: 8px 4px; text-align: left; }}
  code {{ background: #f4f4f4; padding: 2px 5px; border-radius: 4px; }}
  .muted {{ color: #666; font-size: .9rem; }}
</style>
</head>
<body>
<h1>固定订阅</h1>
<p class="muted">由 <a href="{status['source']}">{status['source']}</a> 每日节点自动聚合生成，
最后更新：<strong>{status['updated_at']}</strong></p>
<table>
  <tr><th>候选节点</th><td>{status['collected']} 个（datiya 覆盖 {len(status['days'])} 天）</td></tr>
  {source_row}
  <tr><th>可用节点</th><td>{status['alive']} 个</td></tr>
  {proto_row}
  {line_row}
  {streak_row}
  {china_row}
  {wl_row}
  <tr><th>地区分布</th><td>{regions}</td></tr>
</table>
<table>
  <tr><th>客户端</th><th>订阅文件</th></tr>
  {rows}
</table>
<p class="muted">订阅地址形如 <code>https://&lt;用户名&gt;.github.io/&lt;仓库名&gt;/{out_cfg['clash_file']}</code>，
把它填进 Clash / Mihomo 客户端即可长期使用，无需再更换地址。</p>
</body>
</html>
"""


def write_outputs(out_cfg, clash_text, links, status):
    out_dir = ROOT / out_cfg["dir"]
    clash_path = out_dir / out_cfg["clash_file"]
    v2ray_path = out_dir / out_cfg["v2ray_file"]
    status_path = out_dir / out_cfg["status_file"]

    for path in (clash_path, v2ray_path, status_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    clash_path.write_text(clash_text, encoding="utf-8")
    v2ray_path.write_text(builder.b64("\n".join(links)) + "\n", encoding="utf-8")
    status_path.write_text(
        json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (out_dir / "index.html").write_text(render_page(status, out_cfg), encoding="utf-8")
    log.info("已写入 %s", clash_path.relative_to(ROOT))
    log.info("已写入 %s（%d 条链接）", v2ray_path.relative_to(ROOT), len(links))
    log.info("已写入 %s", status_path.relative_to(ROOT))


def write_candidates(path, proxies, cand_cfg):
    """把本轮候选池写给本机探测（local_probe.py 的输入）。

    候选池必须是「白名单过滤之前」的池子：上次被本机判定不通的节点也要留在里面，
    否则它们永远没机会翻身（免费节点恢复、运营商路由变化都很常见）。
    按连接指纹排序，让相邻两轮的 JSON 行序尽量一致，减小仓库 diff。
    """
    if not cand_cfg.get("enabled", True) or not proxies:
        return 0
    # 只带「构造代理必需」的字段：丢掉名字（本机探测会统一改名 p0001）与空值，
    # 否则 600 个节点的 JSON 约 250KB，而这份文件每次构建都要提交一次。
    items = []
    for proxy in proxies:
        item = {
            key: value
            for key, value in proxy.items()
            if not key.startswith("_")
            and key != "name"
            and value not in (None, "", [], {})
        }
        item["_fp"] = builder.fingerprint(proxy)
        items.append(item)
    items.sort(key=lambda item: item["_fp"])
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone(timedelta(hours=8))).strftime(
            "%Y-%m-%d %H:%M:%S %z"
        ),
        "note": "候选池：供本机 local_probe.py 真实测速用；_fp 为连接指纹",
        "proxies": items,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )
    log.info("已写入 %s（%d 个候选，供本机真实测速）", path.name, len(items))
    return len(items)


def main():
    parser = argparse.ArgumentParser(description="聚合 free.datiya.com 免费节点")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--lookback", type=int, help="覆盖配置中的回溯天数")
    parser.add_argument("--no-check", action="store_true", help="跳过连通性检测")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    cfg = load_config(args.config)

    site, check_cfg, out_cfg = cfg["site"], cfg["check"], cfg["output"]
    lookback = site.get("lookback_days", 3) if args.lookback is None else args.lookback

    fetcher = Fetcher(site)
    days = fetcher.discover_days()[:lookback]
    log.info("本次采集日期：%s", ", ".join(days) or "无")

    proxies, ok_days = collect(fetcher, days)
    extra, source_stats = collect_extra(fetcher, cfg.get("sources"))
    if extra:
        log.info("附加源合计 %d 个节点（%d 个源）", len(extra), len(source_stats))
    proxies.extend(extra)
    if not proxies:
        log.error("未采集到任何节点，保留原有订阅不变")
        return 1

    total = len(proxies)
    proxies = builder.dedupe(proxies)
    deduped = len(proxies)
    log.info("去重后剩余 %d 个节点", deduped)

    proxies = builder.filter_supported(proxies)
    cleaned = len(proxies)
    proxies = builder.sanitize(proxies)
    proxies = builder._unique_names(proxies)

    hist_cfg = cfg.get("history") or {}
    trusted_streak = int(hist_cfg.get("trusted_streak", 3))
    ledger = history.Ledger(
        ROOT / str(hist_cfg.get("file", "history.json")),
        enabled=bool(hist_cfg.get("enabled", True)),
    )

    china_cfg = check_cfg.get("china_check") or {}
    wl_cfg = check_cfg.get("whitelist") or {}
    cand_cfg = check_cfg.get("candidates") or {}
    china_dropped = 0
    wl_used, wl_detail, wl_stats = False, "未启用", {}
    wl_data = None

    if check_cfg.get("enabled", True) and not args.no_check:
        # 白名单新鲜时**跳过境外测速**：它是三重筛选里最不准的一环，而且会误删
        # 本机刚验证可用的节点。实测 167 个白名单节点经美国机房 mihomo 测速后只剩
        # 124 个——被杀掉的 43 个在国内是能用的，这就是 C 档要消除的假阴性。
        # 顺带把单次构建从约 25 分钟压到 3~5 分钟，白名单的时效才追得上节点寿命。
        if wl_cfg.get("enabled", True):
            wl_data, wl_used, wl_detail = whitelist.load_fresh(
                ROOT / str(wl_cfg.get("file", "whitelist.json")), wl_cfg
            )
            log.info("本机白名单：%s", wl_detail)

        if wl_used:
            ledger.annotate(proxies)
            # 完整候选池（白名单过滤之前）：候选池要写回的正是这个池子的抽样，
            # 只写命中节点会让新节点永远没机会被测到，白名单就再也不更新了。
            full_pool = list(proxies)
            proxies, wl_stats = whitelist.apply(full_pool, wl_data, wl_cfg)
            log.info(
                "白名单过滤：去重后 %d → %d（命中 %d；标记失效 %d、不稳定 %d、过慢 %d；"
                "不在白名单的 %d 个未经验证，一律不发）",
                deduped,
                len(proxies),
                wl_stats.get("matched", 0),
                wl_stats.get("dead", 0),
                wl_stats.get("unstable", 0),
                wl_stats.get("slow", 0),
                wl_stats.get("unknown", 0),
            )
            if len(proxies) < int(wl_cfg.get("min_match", 10)):
                log.warning(
                    "白名单只命中 %d 个（低于 min_match=%s），判定与候选池脱节，"
                    "本轮回退 Globalping 探测",
                    len(proxies),
                    wl_cfg.get("min_match", 10),
                )
                wl_used = False
                wl_detail = "命中过少（%d 个），回退 Globalping 探测" % len(proxies)

        if wl_used:
            probed = len(proxies)
            # 发布集只做线路去冗（同 server:port 多份凭据），不做任何截断
            publish = builder.diversify(
                proxies,
                per_line=int(check_cfg.get("max_per_line", 2)),
                trusted_streak=trusted_streak,
            )
            # 候选池 = 本轮发布集 + 未验证的新节点（补足到上限），让本机下一轮有东西可测。
            # 新节点按「存活次数 → 协议可用率」排序，把额度花在最可能成立的候选上。
            proven = {builder.fingerprint(p) for p in proxies}
            rest = [p for p in full_pool if builder.fingerprint(p) not in proven]
            if check_cfg.get("protocol_score"):
                rest = builder.rank_by_protocol(
                    rest, check_cfg["protocol_score"],
                    high_ratio=float(check_cfg.get("high_score_quota", 0.75)),
                    cap=len(rest),
                )
            rest = ledger.rank(rest, trusted_streak)
            candidate_cap = int(cand_cfg.get("max") or 0)
            if candidate_cap:
                need = max(0, candidate_cap - len(publish))
                pool = publish + rest[:need]
                log.info("候选池：发布 %d + 未验证新节点 %d（上限 %d）",
                         len(publish), len(pool) - len(publish), candidate_cap)
            else:
                pool = publish
            write_candidates(
                ROOT / str(cand_cfg.get("file", "candidates.json")), pool, cand_cfg
            )
            proxies = publish
        else:
            if str(check_cfg.get("method", "tcp")).lower() == "mihomo":
                try:
                    proxies = prober.filter_working(proxies, check_cfg)
                except Exception as exc:
                    log.error("mihomo 真实测速不可用（%s），回退为 TCP 检测", exc)
                    proxies = checker.filter_alive(
                        proxies,
                        timeout=check_cfg.get("tcp_timeout", 3.0),
                        concurrency=check_cfg.get("tcp_concurrency", 200),
                    )
            else:
                proxies = checker.filter_alive(
                    proxies,
                    timeout=check_cfg.get("tcp_timeout", 3.0),
                    concurrency=check_cfg.get("tcp_concurrency", 200),
                )
            probed = len(proxies)
            # 存活账本：先标注「连续通过次数」，再稳定优先排序（长寿节点置顶），
            # 再做线路去冗（同一 server:port 的多份凭据只留最好的几个）。
            ledger.annotate(proxies)
            # 协议分区必须在「名额截断」之前：名额给谁，直接决定最终能用几个。
            # 实测 0% 可用的协议占着名额，就等于把额度烧在必死节点上。
            # 配额要按「名额」算而不是按候选总数（候选上千个，按它算等于配额失效）。
            candidate_cap = int(cand_cfg.get("max") or 0)
            protocol_score = check_cfg.get("protocol_score") or {}
            if protocol_score:
                proxies = builder.rank_by_protocol(
                    proxies, protocol_score,
                    high_ratio=float(check_cfg.get("high_score_quota", 0.75)),
                    cap=candidate_cap or int(china_cfg.get("max_units") or 0),
                )
            proxies = ledger.rank(proxies, trusted_streak)
            proxies = builder.diversify(
                proxies,
                per_line=int(check_cfg.get("max_per_line", 2)),
                trusted_streak=trusted_streak,
            )
            if candidate_cap:
                proxies = ledger.select(proxies, candidate_cap, trusted_streak)
            write_candidates(
                ROOT / str(cand_cfg.get("file", "candidates.json")), proxies, cand_cfg
            )
            proxies = ledger.select(
                proxies, int(china_cfg.get("max_units") or 0), trusted_streak
            )

    else:
        log.info("已跳过连通性检测")
        probed = len(proxies)

    # 进入国内探测的候选快照。账本以它为准判断「本轮被探测并淘汰」：
    # 被名额截断、压根没进探测的节点不算淘汰，连续存活次数保持不变（否则会死锁：
    # 没进探测 → 清零 → 下轮没有存活加成 → 永远排不上号）。
    probe_input = list(proxies)

    # 白名单新鲜时，国内可达性已经由本机真实测速回答过了（比 Globalping 的
    # TLS 握手强得多），不再花 Globalping 配额，也不让两种判定互相覆盖。
    if china_cfg.get("enabled") and not args.no_check and not wl_used:
        before_china = len(proxies)
        proxies = china.filter_reachable(proxies, china_cfg)
        china_dropped = before_china - len(proxies)
    elif wl_used:
        log.info("已跳过 Globalping：国内可达性由本机白名单给出")

    # 协议分区已在额度截断前完成（决定谁占用探测名额），这里只记录发布头部协议。
    # 不能再排一次：国内可达性过滤刚把「可达」排在了「未知」前面，重排会打乱这个顺序。
    if (check_cfg.get("protocol_score") or {}) and proxies:
        from collections import Counter as _Counter
        head = _Counter(str(p.get("type")) for p in proxies[:20])
        log.info("发布头部协议构成（前 20）：%s", dict(head))

    log.info(
        "漏斗：采集 %d → 去重 %d → 清洗 %d → 测活 %d → 探测候选 %d → 国内可达 %d",
        total,
        deduped,
        cleaned,
        probed,
        len(probe_input),
        len(proxies),
    )

    if len(proxies) < int(check_cfg.get("min_alive", 30)):
        log.error(
            "可用节点仅 %d 个，低于阈值 %s，保留原有订阅不变",
            len(proxies),
            check_cfg.get("min_alive", 30),
        )
        return 1

    now = datetime.now(timezone(timedelta(hours=8)))
    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S %z"),
        "alive": len(proxies),
        "total": total,
        "days": len(ok_days),
    }
    # 订阅里不能出现 _speed_kbps / _streak 这类内部字段（mihomo 会拒绝整份配置）；
    # 但账本结算要用到它们，所以先造一份干净副本用于发布与校验。
    public = [
        {k: v for k, v in proxy.items() if not k.startswith("_")} for proxy in proxies
    ]
    clash_text = builder.build_clash(public, cfg["clash"], meta)

    ok, detail = prober.validate_config(clash_text, check_cfg)
    if not ok:
        log.error("发布前校验未通过，保留原有订阅不变：\n%s", detail)
        return 1
    log.info("发布前校验：mihomo 可正常加载该配置（%s）", detail)

    links = builder.build_links(public)
    # 只有真正通过发布前校验才记账：本轮发布 +1，进过探测但被淘汰的清零。
    ledger.commit(proxies, probe_input, trusted_streak)

    streaks = [int(p.get("_streak") or 0) for p in proxies]
    wl_age = whitelist.age_hours(wl_data) if wl_data else None
    unique_lines = len({(str(p.get("server")), p.get("port")) for p in proxies})
    status = {
        "updated_at": meta["updated_at"],
        "source": site["base_url"],
        "days": ok_days,
        "collected": total,
        "alive": len(proxies),
        "links": len(links),
        "probe_input": len(probe_input),
        "protocols": dict(
            Counter(str(p.get("type")) for p in proxies).most_common()
        ),
        "unique_lines": unique_lines,
        "line_uniqueness": round(unique_lines / len(proxies), 3) if proxies else 0,
        "streak": {
            "first": sum(1 for s in streaks if s <= 1),
            "repeat": sum(1 for s in streaks if 1 < s < trusted_streak),
            "trusted": sum(1 for s in streaks if s >= trusted_streak),
        },
        "china_check": bool(china_cfg.get("enabled")) and not wl_used,
        "china_dropped": china_dropped,
        "whitelist": {
            "used": wl_used,
            "detail": wl_detail,
            "matched": wl_stats.get("matched", 0),
            "rejected": {
                "dead": wl_stats.get("dead", 0),
                "unstable": wl_stats.get("unstable", 0),
                "slow": wl_stats.get("slow", 0),
            },
            "entries": len(((wl_data or {}).get("entries") or {})),
            # 白名单可能缺 generated_at（文件不存在/损坏），age_hours 会返回 None，
            # 不能直接 round：那会让整轮构建在最后一步崩掉（已踩过）。
            "age_hours": wl_age if wl_age is None else round(wl_age, 2),
            "probe_host": (wl_data or {}).get("probe_host"),
        },
        "sources": source_stats,
        "regions": dict(
            sorted(
                Counter(builder.classify(p["name"]) for p in proxies).items(),
                key=lambda kv: (kv[0] == "其他", kv[0]),
            )
        ),
    }
    write_outputs(out_cfg, clash_text, links, status)
    log.info("完成：%d 个可用节点", len(proxies))
    return 0


if __name__ == "__main__":
    sys.exit(main())
