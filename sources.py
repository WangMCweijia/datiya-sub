"""按采集源统计节点健康度：谁在真供血，谁在灌水。

2026-10-22 实测背景（本机 mihomo 逐节点真实 CONNECT，每源随机 40 个，分批 + 补测）：

    daily_free_vpn  38%   （http 子集 11/14 = 78.6%）
    Au1rxx-07       17.5%
    ermaozi         15%
    Au1rxx-06       10%
    Au1rxx-03/04/05 2.5%  Au1rxx-01/02 0%  TopFreeProxy 0/40  datiya 主源 0/14

线上 300 个节点里 59% 只由 daily_free_vpn 一家独有供养，其余源基本在国家队陪跑。
可候选池名额原来只按协议分配（`check.protocol_score`），完全不知道节点从哪来：
0% 存活率的源照样分走探测额度，等于把最贵的资源花在必死节点上。

这里做两件事：
1. 采集时给每个节点打 `_srcs`（贡献过该节点的源名列表，跨源重复的节点会有多个）；
2. 每轮构建自己产出这张源存活表，写进 `docs/status.json` 与 `source_health.json`
   （后者带滚动 EWMA，为「按源信誉分名额」攒数据，不用再靠手工脚本抽样）。
"""

import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone

log = logging.getLogger("datiya.sources")

SOURCE_KEY = "_srcs"
TZ = timezone(timedelta(hours=8))


def tag(proxies, name):
    """给一批节点标注来源。必须在 dedupe 之前调用（去重会按连接指纹合并来源）。"""
    for proxy in proxies:
        proxy[SOURCE_KEY] = [name]
    return proxies


def _names(proxy):
    names = proxy.get(SOURCE_KEY)
    if isinstance(names, str):
        return [names]
    names = [str(n) for n in (names or [])]
    return names or ["?"]


def tally(proxies):
    """按源计数：节点同时出现在多个源时，每个源各记一次（谁给了它就算谁一份）。"""
    counter = Counter()
    for proxy in proxies:
        for name in _names(proxy):
            counter[name] += 1
    return counter


def tally_own(proxies):
    """只统计「只有一个源贡献」的节点 —— 这才是该源真实的、不可替代的供血能力。"""
    counter = Counter()
    for proxy in proxies:
        names = _names(proxy)
        if len(names) == 1:
            counter[names[0]] += 1
    return counter


def _by_type(pool_proxies, alive_proxies):
    """按 (源, 协议) 拆开计数，返回 {源: {协议: {"pool": n, "alive": m}}}。

    为什么非拆不可：同一个源里 http 和 vless 的命运是两回事。2026-10-22 抽样说
    TopFreeProxy「40 个 0 存活」，可它在候选池里的 http 子集却有 56% 通过 ——
    它灌的 2647 个节点绝大多数是 vless/vmess，把整体拉成 0%。只看源级总数，
    要么误杀（把它的 http 一起砍）、要么误信（以为整源都能用）。
    """
    table = {}
    for bucket, proxies in (("pool", pool_proxies), ("alive", alive_proxies)):
        for proxy in proxies:
            kind = str(proxy.get("type") or "?").lower()
            for name in _names(proxy):
                cell = table.setdefault(name, {}).setdefault(kind, {"pool": 0, "alive": 0})
                cell[bucket] += 1
    return table


def build(collected, own, pool, alive):
    """合成源健康表。

    四列的口径（缺一列就会得出错误结论）：
      collected 本轮该源解析到的原始节点数（灌水量）
      own       去重后「只有这个源有」的节点数（真实增量）
      pool      进入候选池、真正被本机测过的节点数（花了多少探测预算）
      alive     最终进入发布订阅的节点数（真供血）
    `rate = alive / pool`：花在该源上的探测预算有多少变成了可用节点。
    单看 collected 会把 TopFreeProxy（2647 个、抽样 0 存活）当成主力源。
    """
    pool_counts, alive_counts = tally(pool), tally(alive)
    names = set(collected) | set(own) | set(pool_counts) | set(alive_counts)
    types = _by_type(pool, alive)
    names |= set(types)
    rows = []
    for name in names:
        got = int(collected.get(name, 0) or 0)
        mine = int(own.get(name, 0) or 0)
        probed = int(pool_counts.get(name, 0) or 0)
        ok = int(alive_counts.get(name, 0) or 0)
        cells = types.get(name, {})
        by_type = {
            kind: {
                "pool": cell["pool"],
                "alive": cell["alive"],
                "rate": round(cell["alive"] / cell["pool"], 3) if cell["pool"] else 0.0,
            }
            for kind, cell in sorted(cells.items())
        }
        rows.append(
            {
                "name": name,
                "collected": got,
                "own": mine,
                "pool": probed,
                "alive": ok,
                "rate": round(ok / probed, 3) if probed else 0.0,
                "by_type": by_type,
            }
        )
    rows.sort(key=lambda row: (-row["alive"], -row["rate"], -row["collected"]))
    return rows


def update_history(path, rows, alpha=0.5):
    """把本轮命中率滚进 EWMA，返回 {源名: 平滑后的命中率}。

    免费源的存活率是小时级波动的（同一批节点相邻两轮 64% → 34%），单轮数字不足以
    给源「定级」。留一份滚动值，将来按源信誉分名额时直接可用。
    """
    data = {"version": 1, "sources": {}}
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8")) or data
    except Exception as exc:  # 文件损坏不该让整轮构建失败
        log.warning("源健康历史读取失败（%s），本轮重建", exc)
    entries = data.setdefault("sources", {})
    stamp = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %z")
    ewma = {}
    for row in rows:
        entry = entries.setdefault(row["name"], {})
        prev = entry.get("ewma")
        value = (
            float(row["rate"])
            if prev is None
            else round(float(alpha) * float(row["rate"]) + (1 - float(alpha)) * float(prev), 4)
        )
        entry.update(
            {
                "rate": row["rate"],
                "ewma": value,
                "alive": row["alive"],
                "pool": row["pool"],
                "rounds": int(entry.get("rounds") or 0) + 1,
                "last_seen": stamp,
            }
        )
        ewma[row["name"]] = value
    # 本轮没出现的源保留历史（可能是临时 404），但不再影响定级
    data["updated_at"] = stamp
    data["sources"] = {k: entries[k] for k in sorted(entries)}
    try:
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
    except OSError as exc:
        log.warning("源健康历史写入失败（%s）", exc)
    return ewma


def format_rows(rows, limit=0):
    """日志/页面用的紧凑一行：源 存活/候选 (命中率)。"""
    shown = rows if not limit else rows[:limit]
    return "、".join(
        "%s %d/%d (%.0f%%)" % (r["name"], r["alive"], r["pool"], r["rate"] * 100)
        for r in shown
    )
