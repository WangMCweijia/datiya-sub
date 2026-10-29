"""free.datiya.com 免费节点聚合：采集 -> 测活 -> 合并 -> 输出固定订阅。"""

import argparse
import gzip
import json
import logging
import os
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
import sources
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


def _whitelist_hold(wl_cfg, wl_data, reason):
    """白名单不可用时默认「不发」——保留上一版订阅，而不是回退机房测速。

    2026-10-28 踩过：本机 18:15 之后断电，白名单过了 4 小时有效期，CI 按旧逻辑
    回退 Globalping（境外机房）测速，连续两轮（23:01 / 05:08）发出中位 8.9MB/s、
    最快 80MB/s 的节点。本机复测那批「标称最快 30 个」：只有 1 个能出网、43.9KB/s，
    标称/实测中位 875 倍。机房测的是「节点到机房的带宽」，与本机可达性的秩相关
    只有 +0.107，两种数不能混用——所以宁可订阅停在几小时前那一版，也不发假数字。

    只有「白名单文件缺失 / 无条目」继续走 Globalping：那时没有可保留的上一版，
    新仓库也需要先有东西可发。把 `whitelist.on_stale` 设成 `globalping` 可恢复旧行为。

    返回 True 表示本轮应当直接结束（不写任何产物）。
    """
    if str(wl_cfg.get("on_stale", "hold") or "hold").lower() != "hold":
        return False
    if not wl_data or not (wl_data.get("entries") or {}):
        return False
    log.error(
        "白名单不可用（%s）→ 按 whitelist.on_stale=hold 保留上一版订阅，本轮不发布；"
        "本机跑一次 local_probe.py --push 即可恢复更新",
        reason,
    )
    if os.environ.get("GITHUB_ACTIONS"):
        # Actions 里会变成一条 annotation，不用翻日志也能看见「为什么今天没更新」
        print("::warning title=订阅已暂停更新::%s" % reason)
    return True


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
        proxies.extend(sources.tag(parsed, "datiya"))
        ok_days.append(day)
    return proxies, ok_days


def collect_extra(fetcher, sources_cfg):
    """采集附加订阅源，返回 (节点列表, {源名: 节点数})。"""
    proxies, stats = [], {}
    for src in sources_cfg or []:
        name = src.get("name") or src.get("url", "?")
        text = fetcher.fetch_text(src["url"])
        if not text:
            log.warning("附加源 %s：未取到内容，跳过", name)
            stats[name] = 0
            continue
        parsed = builder.parse_proxies(text)
        log.info("附加源 %s：解析到 %d 个节点", name, len(parsed))
        stats[name] = len(parsed)
        proxies.extend(sources.tag(parsed, name))
    return proxies, stats


def render_page(status, out_cfg):
    rows = "".join(
        f"<tr><td>{label}</td><td><code>{path}</code></td></tr>"
        for label, path in (
            ("Clash / Mihomo", out_cfg["clash_file"]),
            ("v2rayN / v2rayNG", out_cfg["v2ray_file"]),
        )
    )
    # 地区从 16 个扩到 100+ 之后，「地区分布」会有几十项，整行塞进去反而看不清；
    # 只列节点最多的 12 个，其余折成一句「另 N 个地区」。
    region_items = list((status.get("regions") or {}).items())
    head = "、".join(f"{k} {v}" for k, v in region_items[:12])
    rest = len(region_items) - 12
    regions = (head + (f"，另 {rest} 个地区" if rest > 0 else "")) or "无"
    sources = status.get("sources") or {}
    source_row = ""
    if sources:
        # 这里含 datiya 主源（同样按源记账），所以叫「采集源」而不是「附加源」
        detail = "、".join(f"{name} {cnt}" for name, cnt in sources.items())
        source_row = f"<tr><th>采集源</th><td>{len(sources)} 个（{detail}）</td></tr>"
    health_row = ""
    rows = (status.get("source_health") or {}).get("rows") or []

    def _type_note(row):
        # 同一源里 http 与 vless 命运完全不同，所以除了总命中率还拆协议；
        # 样本 <5 的格子不展示（纯噪声）。
        cells = [(kind, cell) for kind, cell in (row.get("by_type") or {}).items()
                 if cell.get("pool", 0) >= 5]
        cells.sort(key=lambda kv: -kv[1]["alive"])
        return "、".join(f"{kind} {cell['alive']}/{cell['pool']}" for kind, cell in cells)

    if rows:
        # 只列真正供血的源（发布 > 0）：把一堆 0 的源并进来，页面就变成灌水展览了。
        live = [r for r in rows if r.get("alive")]
        dead = [r["name"] for r in rows if not r.get("alive")]
        detail = "、".join(
            f"{r['name']} {r['alive']}/{r['pool']}"
            f"（{r['rate']:.0%}，独家 {r['own']}"
            + (f"；{_type_note(r)}" if _type_note(r) else "")
            + "）"
            for r in live
        ) or "无"
        dead_note = (
            f"<br><span class=\"muted\">本轮零供血：{'、'.join(dead)}</span>" if dead else ""
        )
        health_row = (
            f"<tr><th>源存活</th><td>{detail}{dead_note}"
            f"<br><span class=\"muted\">口径：发布 / 进入候选池（本机实测命中率），"
            f"独家 = 去重后只有该源提供的节点数；括号内按协议拆分</span></td></tr>"
        )
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
        floor = wl.get("min_kbps") or 0
        floor_txt = (
            f"；网速下限 {floor:g}KB/s（测不出速度的同样剔除）"
            if floor and not wl.get("floor_bypassed")
            else ""
        )
        wl_row = (
            f"<tr><th>国内实测</th><td>{wl.get('probe_host') or '本机'}"
            f"真实代理测速白名单，{wl.get('age_hours') or 0:.1f} 小时前生成；"
            f"剔除 失效 {rejected.get('dead', 0)} · 不稳定 {rejected.get('unstable', 0)}"
            f" · 过慢 {rejected.get('slow', 0)}"
            f" · 测不出速度 {rejected.get('unmeasured', 0)}"
            f" · 只能明文 {rejected.get('no_https', 0)}{floor_txt}</td></tr>"
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
    # 主源停用后不能再写「由 free.datiya.com 每日节点聚合生成」——那句话会直接误导
    # 读者以为是站点在供血（实际 0 个节点来自它）。
    source = status.get("source") or ""
    if status.get("site_enabled", True):
        intro = f'由 <a href="{source}">{source}</a> 每日节点自动聚合生成'
        day_note = f"（datiya 覆盖 {len(status['days'])} 天）"
    else:
        intro = "由多个公开订阅源自动聚合生成"
        day_note = "（仅附加源）"

    rank = status.get("rank") or {}
    rank_row = ""
    if rank.get("median_kbps") is not None:
        # 老黄牛这一层的门槛写在页面上，读者才知道「排在前面的不是最快的那个」为什么。
        vet_days = float(rank.get("veteran_days") or 0)
        vet_kbps = float(rank.get("veteran_kbps") or 0)
        rule = "仅按实测网速，快 → 慢"
        if vet_days > 0 and vet_kbps > 0:
            rule = (
                f"存活 ≥{vet_days:g} 天且网速 ≥{vet_kbps:g}KB/s 的节点前置，"
                f"其余按实测网速快 → 慢"
            )
        # 网速下限生效时列表里已经没有 `--`，再写「其余为 -- 排在末尾」是自相矛盾
        floor_now = float(((status.get("whitelist") or {}).get("min_kbps")) or 0)
        tail_txt = (
            "测不出速度的已按网速下限剔除）"
            if floor_now
            else "其余为 -- 排在末尾）"
        )
        rank_row = (
            f"<tr><th>排序口径</th><td>{rule}"
            f"（测出网速的 {rank['measured']}/{rank['total']} 个：中位 "
            f"{rank['median_kbps']:.0f}KB/s、最快 {rank['best_kbps']:.0f}KB/s；"
            f"{tail_txt}</td></tr>"
        )
        if vet_days > 0 and vet_kbps > 0:
            rank_row += (
                f"<tr><th>存活</th><td>名字里第二个数字是已存活时长（首次测通至今，"
                f"带 ~ 为按探测轮次推算）；当前达标老黄牛 "
                f"<strong>{rank.get('veterans', 0)}</strong> 个，存活中位 "
                f"{float(rank.get('median_age_h') or 0):.1f} 小时、"
                f"最久 {float(rank.get('oldest_age_h') or 0):.1f} 小时</td></tr>"
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
<p class="muted">{intro}，
最后更新：<strong>{status['updated_at']}</strong></p>
<table>
  <tr><th>候选节点</th><td>{status['collected']} 个{day_note}</td></tr>
  {source_row}
  {health_row}
  <tr><th>可用节点</th><td>{status['alive']} 个</td></tr>
  {proto_row}
  {line_row}
  {streak_row}
  {rank_row}
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
    # 1200 个节点紧凑 JSON 约 285KB，而这份文件每轮构建都要提交一次；gzip 后约 48KB。
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if str(path).endswith(".gz"):
        path.write_bytes(gzip.compress(blob, 9))
    else:
        path.write_bytes(blob + b"\n")
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
    # site.enabled=false：主源停用（实测零存活、零独家），只走附加源。
    # 用「先置空再分支」而不是把 collect 包在 if 里，是为了让后面的
    # source_stats / ok_days / status 都不必再判一次空。
    site_on = bool(site.get("enabled", True)) and lookback > 0
    if site_on:
        days = fetcher.discover_days()[:lookback]
        log.info("本次采集日期：%s", ", ".join(days) or "无")
        proxies, ok_days = collect(fetcher, days)
        log.info("主源 %s：合计 %d 个节点", site.get("base_url", ""), len(proxies))
    else:
        proxies, ok_days = [], []
        log.info(
            "主源 %s 已停用（site.enabled=false），本轮只用 %d 个附加源",
            site.get("base_url", ""),
            len(cfg.get("sources") or []),
        )

    extra, source_stats = collect_extra(fetcher, cfg.get("sources"))
    if extra:
        log.info("附加源合计 %d 个节点（%d 个源）", len(extra), len(source_stats))
    # datiya 主源也算一个源，单独记账：它 7 天只有 98 个节点、实测 0 存活，
    # 混进总数就看不出「谁在真供血」了。停用后不再记这一行，否则源健康表里
    # 会永远挂着一个 collected=0 的幽灵源。
    if site_on:
        source_stats = {"datiya": len(proxies), **source_stats}
    proxies.extend(extra)
    if not proxies:
        log.error("未采集到任何节点，保留原有订阅不变")
        return 1

    total = len(proxies)
    proxies = builder.dedupe(proxies)
    deduped = len(proxies)
    log.info("去重后剩余 %d 个节点", deduped)
    # 源的真实增量 = 去重后「只有这个源有」的节点数（跨源重复的算共享，不算谁的功劳）
    source_own = sources.tally_own(proxies)

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
    wl_cfg = dict(check_cfg.get("whitelist") or {})
    # 网速下限只留一个口径：`check.min_speed_kbps`（白名单与境外测速两条路径共用）。
    # 两处各写一个数字的结果是「改了白名单那个，回退到 Globalping 时又换了一套标准」，
    # 用户看到的节点集合会随「白名单是否新鲜」跳变，很难解释。
    if not wl_cfg.get("min_kbps"):
        wl_cfg["min_kbps"] = check_cfg.get("min_speed_kbps", 0)
    # HTTPS 闸门同理只留一个开关：本机探测按 `check.https_check` 测不测，
    # 发布侧按 `check.whitelist.require_https`（未显式设置时跟随前者）筛不筛。
    wl_cfg.setdefault("require_https", check_cfg.get("https_check", True))
    wl_cfg.setdefault("https_fail_rounds", check_cfg.get("https_fail_rounds", 2))
    cand_cfg = check_cfg.get("candidates") or {}
    # 发布顺序：只按实测网速（见 builder.sort_by_speed）。只影响订阅里
    # 节点的排列顺序，不淘汰节点，所以两条测活分支（白名单 / Globalping）都能用。
    rank_cfg = check_cfg.get("rank") or {}
    china_dropped = 0
    wl_used, wl_detail, wl_stats = False, "未启用", {}
    wl_data = None
    # 本轮真正写进候选池的节点（本机下一轮会测的就是它们）。源健康表要用它当分母：
    # 拿采集数当分母会把「压根没被测过」算成失败。
    cand_pool = []

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
            # 过期/损坏 → 直接收工（保留上一版订阅），不再回退到机房测速。
            if not wl_used and _whitelist_hold(wl_cfg, wl_data, wl_detail):
                return 0

        if wl_used:
            ledger.annotate(proxies)
            # 完整候选池（白名单过滤之前）：候选池要写回的正是这个池子的抽样，
            # 只写命中节点会让新节点永远没机会被测到，白名单就再也不更新了。
            full_pool = list(proxies)
            proxies, wl_stats = whitelist.apply(full_pool, wl_data, wl_cfg)
            log.info(
                "白名单过滤：去重后 %d → %d（命中 %d；标记失效 %d、不稳定 %d、"
                "过慢 %d、测不出网速 %d、只能明文 %d、超出实测窗口 %d；"
                "不在白名单的 %d 个未经验证，一律不发）",
                deduped,
                len(proxies),
                wl_stats.get("matched", 0),
                wl_stats.get("dead", 0),
                wl_stats.get("unstable", 0),
                wl_stats.get("slow", 0),
                wl_stats.get("unmeasured", 0),
                wl_stats.get("no_https", 0),
                wl_stats.get("stale", 0),
                wl_stats.get("unknown", 0),
            )
            if wl_stats.get("floor_bypassed"):
                log.warning(
                    "网速下限 %sKB/s 本轮未生效（达标节点过少，判定测速环节异常）",
                    wl_cfg.get("min_kbps"),
                )
            # min_match 判的是「白名单与候选池是否脱节」，所以比的是**命中数**，
            # 不是网速过滤后的发布数——否则下限一收紧，正常的一轮也会被误判成
            # 「脱节」并回退到 35 分钟的 Globalping 探测。
            if wl_stats.get("matched", 0) < int(wl_cfg.get("min_match", 10)):
                reason = "命中过少（%d 个 < min_match=%s），白名单与候选池脱节" % (
                    wl_stats.get("matched", 0),
                    wl_cfg.get("min_match", 10),
                )
                log.warning("%s", reason)
                # 白名单是新鲜的、只是和采集池对不上，说明该重跑本机探测而不是
                # 换成机房口径——后者发出去的网速国内用户一个都跑不到。
                if _whitelist_hold(wl_cfg, wl_data, reason):
                    return 0
                wl_used = False
                wl_detail = reason + "，回退 Globalping 探测"

        if wl_used:
            probed = len(proxies)
            # 发布集只做线路去冗（同 server:port 多份凭据），不做任何截断
            publish = builder.diversify(
                proxies,
                per_line=int(check_cfg.get("max_per_line", 2)),
                trusted_streak=trusted_streak,
            )
            # 预排序：只按实测网速（白名单条目里带了 _wl_kbps）。
            # 这里先排一次，是为了让写进候选池的发布集顺序也是网速序——本机下一轮按
            # 顺序取前 N 个重测，快的节点先测更划算。最终发布顺序在漏斗末尾统一再排。
            publish = builder.sort_by_speed(publish, rank_cfg)
            # 候选池 = 本轮发布集 + 白名单已验证的老节点 + 未验证的新节点（补足到上限）。
            # 三层的顺序不能颠倒：
            #   ① publish  —— 本轮正在发布的，必须下一轮复测（否则自己也变 stale）；
            #   ② verified —— 白名单里测通过、但本轮因轮次/网速/名额掉出去的老节点。
            #      它们不占位就永远没机会重测，只能被判 stale，见 builder.split_verified；
            #   ③ fresh    —— 从没测过的节点，按协议分区排（低分协议的探索名额靠它）。
            # verified 拿 `verified_ratio` 的配额，余量给 fresh：既保证老节点滚动复测，
            # 也不让候选池退化成「只有老面孔」、新血进不来。
            proven = {builder.fingerprint(p) for p in proxies}
            rest = [p for p in full_pool if builder.fingerprint(p) not in proven]
            candidate_cap = int(cand_cfg.get("max") or 0)
            verified_ratio = float(cand_cfg.get("verified_ratio", 0) or 0)
            verified, fresh = builder.split_verified(
                rest, (wl_data or {}).get("entries") or {}
            )
            if check_cfg.get("protocol_score"):
                fresh = builder.rank_by_protocol(
                    fresh, check_cfg["protocol_score"],
                    high_ratio=float(check_cfg.get("high_score_quota", 0.75)),
                    cap=len(fresh),
                    source_scores=check_cfg.get("source_score"),
                    explore_per_proto=int(check_cfg.get("protocol_explore_per_type") or 0),
                )
            verified = ledger.rank(verified, trusted_streak)
            fresh = ledger.rank(fresh, trusted_streak)
            if candidate_cap:
                need = max(0, candidate_cap - len(publish))
                # ratio=0 时退化为「老节点全保」，只在新老之间不设上限（旧行为）
                quota = int(round(need * verified_ratio)) if verified_ratio > 0 else need
                take_verified = verified[: max(0, min(quota, need))]
                take_fresh = fresh[: max(0, need - len(take_verified))]
                pool = publish + take_verified + take_fresh
                log.info(
                    "候选池：发布 %d + 已验证老节点 %d/%d + 新节点 %d/%d（上限 %d）",
                    len(publish), len(take_verified), len(verified),
                    len(take_fresh), len(fresh), candidate_cap,
                )
                if len(verified) > len(take_verified):
                    log.info(
                        "候选池：%d 个已验证老节点因配额没排上（verified_ratio=%.2f），"
                        "它们本轮不会被重测",
                        len(verified) - len(take_verified), verified_ratio,
                    )
            else:
                pool = publish
            write_candidates(
                ROOT / str(cand_cfg.get("file", "candidates.json")), pool, cand_cfg
            )
            cand_pool = pool
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
                    source_scores=check_cfg.get("source_score"),
                    explore_per_proto=int(check_cfg.get("protocol_explore_per_type") or 0),
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
            cand_pool = list(proxies)
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

    # 发布顺序在这里定稿：只按实测网速从快到慢。
    # 上面各步骤的排序（协议分区、存活优先、国内可达优先）服务的是「谁占探测名额」，
    # 与「用户打开订阅看到的顺序」是两件事——名额分配完之后，展示顺序就该按网速排。
    # 只改顺序、不淘汰节点，所以不影响漏斗任何一级的计数。
    proxies = builder.sort_by_speed(proxies, rank_cfg)
    rank_info = builder.rank_summary(proxies, rank_cfg)
    log.info(
        "发布顺序：老黄牛（存活 ≥%s 天且 ≥%sKB/s）前置 %d 个，其余按实测网速降序%s",
        rank_cfg.get("veteran_days", 0),
        rank_cfg.get("veteran_kbps", 0),
        rank_info.get("veterans", 0),
        "；网速中位 %.0fKB/s，最好 %.0fKB/s，测出网速 %d/%d，存活中位 %.1fh"
        % (
            rank_info.get("median_kbps", 0),
            rank_info.get("best_kbps", 0),
            rank_info.get("measured", 0),
            rank_info.get("total", 0),
            rank_info.get("median_age_h", 0),
        )
        if rank_info
        else "（无网速实测，保持上游顺序）",
    )

    # 统一改名：地区 | 网速 | 原名代表性标志。
    # 必须排在测速与排序之后——网速是名字的一部分，测完才有值；顺序也定了，
    # 名字里的数字才和用户看到的排列一致。改名不影响任何计数与去重（去重看连接指纹）。
    naming_cfg = cfg.get("naming") or {}
    proxies = builder.rename(proxies, naming_cfg)
    if proxies:
        log.info(
            "节点命名：地区 | 网速 | 已存活 | 代表性标志，例如 %s",
            "；".join(p["name"] for p in proxies[:3]),
        )

    # 协议分区已在额度截断前完成（决定谁占用探测名额），这里只记录发布头部协议。
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

    # 发布数下限有两套口径，必须分开：
    #   · 默认 min_alive(30)：防的是「采集/探测环节故障」，比如源站挂了、内核起不来；
    #   · 因网速下限主动筛掉时用 min_speed_keep(5)：节点少是**筛出来的结果**，
    #     不是故障。用 30 去卡它，会让「按 100KB/s 过滤」这一整轮白跑——
    #     筛后剩 7 个 → 判不合格 → 保留旧订阅 → 用户看到的还是那一批慢节点。
    # 「只能明文」也属于**主动筛掉**（不是探测故障），所以同样算进 floor_cut，
    # 否则某轮 HTTPS 闸门筛得多时会被 min_alive=30 拦下、保留旧订阅，闸门等于没生效
    # —— 与网速下限踩过的是同一个坑。
    floor_cut = (int(wl_stats.get("slow", 0)) + int(wl_stats.get("unmeasured", 0))
                 + int(wl_stats.get("no_https", 0)))
    alive_floor = int(check_cfg.get("min_alive", 30))
    if floor_cut and int(wl_cfg.get("min_kbps") or 0) > 0:
        alive_floor = min(alive_floor, int(check_cfg.get("min_speed_keep", 5)))
    if len(proxies) < alive_floor:
        log.error(
            "可用节点仅 %d 个，低于阈值 %s，保留原有订阅不变",
            len(proxies),
            alive_floor,
        )
        return 1

    now = datetime.now(timezone(timedelta(hours=8)))
    # 本轮实际生效的网速下限：白名单兜底（floor_bypassed）时等于 0，头部就不该
    # 声称「已剔除」——否则读者会以为列表里没有 `--`，实际有。
    if wl_used:
        active_floor = 0.0 if wl_stats.get("floor_bypassed") else float(
            wl_cfg.get("min_kbps") or 0
        )
    else:
        active_floor = float(check_cfg.get("min_speed_kbps") or 0)
    # 网速口径要跟着订阅一起发出去：只有白名单真的用上了，名字里的数字才是
    # 「这条线实测」；回退到机房探测时必须让读者知道那些数只是估算。
    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S %z"),
        "alive": len(proxies),
        "total": total,
        "days": len(ok_days),
        "site_enabled": site_on,
        "wl_age": whitelist.age_hours(wl_data) if (wl_used and wl_data) else None,
        "wl_host": (wl_data or {}).get("probe_host") if wl_used else None,
        # 排序口径要写进订阅头：读者需要知道「排在最前的是为什么排最前」。
        "veteran_days": rank_cfg.get("veteran_days", 0),
        "veteran_kbps": rank_cfg.get("veteran_kbps", 0),
        "min_kbps": active_floor,
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

    # ---- 源健康度：每轮构建自己产出，不再靠手工抽样脚本 ----
    health_cfg = cfg.get("source_health") or {}
    source_rows = sources.build(source_stats, source_own, cand_pool, proxies)
    if health_cfg.get("enabled", True) and source_rows:
        summary = sources.format_rows(source_rows)
        log.info("源健康度（发布/候选）：%s", summary)
        sources.update_history(
            ROOT / str(health_cfg.get("file", "source_health.json")),
            source_rows,
            alpha=float(health_cfg.get("ewma_alpha", 0.5)),
        )

    status = {
        "updated_at": meta["updated_at"],
        "source": site["base_url"],
        "site_enabled": site_on,
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
        # 发布顺序用的口径与网速分布（便于核对「排序是否按预期生效」）
        "rank": rank_info,
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
                # 测不出网速（kbps=0）的单独计数：它和「慢」不是一回事——
                # 实测这类节点 77% 是「204 探测能过、一下载大文件就被上游掐断」。
                "unmeasured": wl_stats.get("unmeasured", 0),
                # 「能代理明文、打不开 https」：名字里可能写着几 MB/s，用户点开网页转圈。
                "no_https": wl_stats.get("no_https", 0),
                "stale": wl_stats.get("stale", 0),
            },
            "min_kbps": active_floor,
            "floor_bypassed": bool(wl_stats.get("floor_bypassed")),
            "entries": len(((wl_data or {}).get("entries") or {})),
            # 白名单可能缺 generated_at（文件不存在/损坏），age_hours 会返回 None，
            # 不能直接 round：那会让整轮构建在最后一步崩掉（已踩过）。
            "age_hours": wl_age if wl_age is None else round(wl_age, 2),
            "probe_host": (wl_data or {}).get("probe_host"),
        },
        "sources": source_stats,
        "source_health": {
            "generated_at": meta["updated_at"],
            "rows": source_rows,
        },
        # 按节点数从多到少（「其他」置底）：地区和分组用的是同一套口径，
        # 一眼就能对上订阅里的 🌐 分组。
        "regions": dict(
            sorted(
                Counter(builder.classify(p["name"]) for p in proxies).items(),
                key=lambda kv: (kv[0] == "其他", -kv[1], kv[0]),
            )
        ),
        # 命名口径与样例：便于一眼核对「名字里的地区/网速」是否来自实测
        "naming": {
            "enabled": bool((cfg.get("naming") or {}).get("enabled", True)),
            "format": "地区|网速|代表性标志",
            "sample": [p["name"] for p in proxies[:5]],
        },
    }
    write_outputs(out_cfg, clash_text, links, status)
    log.info("完成：%d 个可用节点", len(proxies))
    return 0


if __name__ == "__main__":
    sys.exit(main())
