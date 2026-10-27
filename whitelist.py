"""C 档：用「国内家宽真实代理测速」的白名单替代境外推断。

为什么必须这么做
----------------
CI 的两道筛选都不在国内，谁都没验证过「国内家宽能不能真的代理出网」：

- mihomo 测速跑在 GitHub Actions（美国机房）：只能证明「境外连得上」；
- Globalping 只发一次 TLS 握手：GFW 常「放握手、掐流量」，握上不等于能传数据。

实测（2026-10-21，本机 mihomo 逐节点真实 CONNECT）：通过全部筛选的 169 个节点里
只有 78 个真能用（46%）；而同一批节点在本机测出的可用率差异极大——http 84%、
vless 10%。也就是说「境外能连」与「国内能用」几乎是两件事。

做法
----
由用户自己的国内机器运行 `local_probe.py`：拿 CI 产出的 `candidates.json`，
用 mihomo 内核逐节点真实 CONNECT + 下载测速，写出 `whitelist.json` 推回仓库。
CI 每轮按白名单过滤，并按本机实测网速排序（订阅里靠前的就是真正快的）。

安全性
------
- 白名单带时间戳，超过 `max_age_hours`（默认 6 小时）视为过期，自动退回原有
  Globalping 探测，不会因为本机没开机而写坏订阅；
- 白名单里一个节点都没匹配上时同样回退（源站换了一批全新节点等）；
- 过滤后不足 `min_alive` 时由 main 拦住，保留线上旧订阅。
"""

import logging
from datetime import datetime, timedelta, timezone

import builder

log = logging.getLogger("datiya.whitelist")

TZ = timezone(timedelta(hours=8))
VERSION = 1
MAX_ENTRIES = 1000


def _now():
    return datetime.now(TZ)


def stamp(dt=None):
    return (dt or _now()).strftime("%Y-%m-%d %H:%M:%S %z")


def empty(host=""):
    return {
        "version": VERSION,
        "generated_at": None,
        "probe_host": host,
        "probe_rounds": 0,
        "entries": {},
    }


def load(path):
    """读白名单；不存在/损坏都返回空结构（调用方据 generated_at 判断新鲜度）。"""
    import json

    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return empty()
    except OSError as exc:
        log.warning("白名单读取失败（%s）", exc)
        return empty()
    try:
        data = json.loads(text)
    except ValueError as exc:
        log.warning("白名单 JSON 损坏（%s），按无白名单处理", exc)
        return empty()
    if not isinstance(data, dict):
        return empty()
    data.setdefault("version", VERSION)
    data.setdefault("entries", {})
    return data


def age_hours(data):
    """白名单生成到现在的小时数；无时间戳返回 None。"""
    raw = (data or {}).get("generated_at")
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            when = datetime.strptime(str(raw), fmt)
        except ValueError:
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=TZ)
        return (_now() - when).total_seconds() / 3600.0
    return None


def parse_stamp(text):
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            when = datetime.strptime(str(text), fmt)
        except ValueError:
            continue
        return when if when.tzinfo else when.replace(tzinfo=TZ)
    return None


def load_fresh(path, cfg):
    """返回 (data, 是否可用, 说明)。说明用于日志和 status.json。"""
    data = load(path)
    entries = int((data.get("entries") or {}).__len__())
    hours = age_hours(data)
    if not entries:
        return data, False, "无白名单文件（%s），回退 Globalping 探测" % path.name
    max_age = float(cfg.get("max_age_hours", 6) or 0)
    if hours is None:
        return data, False, "白名单缺 generated_at，无法判断新鲜度，回退 Globalping 探测"
    if hours < 0:
        return data, False, "白名单时间戳在未来（本机时钟偏差？），回退 Globalping 探测"
    if max_age and hours > max_age:
        return (
            data,
            False,
            "白名单已过期（%.1f 小时 > %.0f 小时上限），回退 Globalping 探测；"
            "请在本机运行 local_probe.py --push" % (hours, max_age),
        )
    return data, True, "白名单可用：%d 条记录，%.1f 小时前由 %s 实测" % (
        entries,
        hours,
        data.get("probe_host") or "本机",
    )


def _sort_key(proxy):
    """粗略兜底排序：测出网速的按网速从快到慢，其余沉底。

    发布时的实际顺序由 main 调用 `builder.sort_by_speed` 定稿，这里只保证
    「不经过发布流程的调用方」（测试、脚本）拿到的不是随机顺序。同网速保持原顺序
    （sort 稳定），不再用延迟兜底——排序口径只有一个维度。
    """
    kbps = float(proxy.get("_wl_kbps") or 0)
    return (0 if kbps > 0 else 1, -kbps)


def estimate_age_hours(entry, cfg):
    """没有真实首次测通时间戳时，用「累计测通轮数 × 轮间隔」倒推存活时长。

    为什么需要这个兜底：本功能上线前的白名单条目只有 `ok_rounds`，没有 `first_ok`。
    不推算的话全部节点都显示 0h、老黄牛优先级永远不触发，得等三天数据攒出来才有
    意义。推算值在名字里带 `~`（见 builder.age_estimated），读者能一眼分辨。

    口径警告：`ok_rounds` 是**累计**测通轮数而非连续，中间断过的节点推算值会偏大，
    所以它只配当「至少活了这么久」的量级参考。轮间隔取本机探测周期
    （`round_interval_hours`，与计划任务一致）。
    """
    hours = builder.hours_since(entry.get("first_ok"))
    if hours is not None:
        return hours
    interval = float(cfg.get("round_interval_hours", 3) or 0)
    if interval <= 0:
        return None
    rounds = int(entry.get("ok_rounds") or 0)
    base = builder.hours_since(entry.get("last_ok"))
    if base is None or rounds <= 0:
        return None
    return base + (rounds - 1) * interval


def apply(proxies, data, cfg):
    """按白名单过滤并按实测质量排序，返回 (节点列表, 统计)。"""
    entries = (data or {}).get("entries") or {}
    min_ok = int(cfg.get("min_ok_rounds", 1) or 1)
    min_kbps = float(cfg.get("min_kbps", 0) or 0)
    # 实测时间窗：ok_rounds 是累计值，一个节点一小时前测通、现在早已失效仍是 ≥1。
    # 免费节点寿命以分钟计，超过这个窗口没再测通的条目一律不发（本机会持续补测，
    # 真还活着的节点几分钟内就会被重新测到并刷新 last_ok）。
    window_h = float(cfg.get("ok_within_hours", 1) or 0)
    ref = parse_stamp((data or {}).get("generated_at"))
    # 只发「本轮实测通过」的节点：round 是 local_probe 每轮的序号，
    # 比时间窗精确得多（一轮要测好几分钟，时间窗无法区分同一轮内外的结果）。
    latest = int((data or {}).get("probe_rounds") or 0)
    # 老格式（升级前的 whitelist.json）没有 round 字段：这时不能强行按轮过滤，
    # 否则所有条目都被判 stale、发布 0 个，构建只因「还没来得及重测」就红掉（踩过）。
    has_round = any("round" in entry for entry in entries.values())
    enforce_round = bool(latest) and has_round and cfg.get("require_latest_round", True)

    kept, stats = [], {"matched": 0, "dead": 0, "unstable": 0, "slow": 0,
                       "stale": 0, "unknown": 0}
    for proxy in proxies:
        entry = entries.get(builder.fingerprint(proxy))
        if not entry:
            stats["unknown"] += 1
            continue
        stats["matched"] += 1
        if entry.get("dead"):
            stats["dead"] += 1
            continue
        if enforce_round:
            if int(entry.get("round") or 0) != latest:
                stats["stale"] += 1
                continue
        if window_h and ref:
            last_ok = parse_stamp(entry.get("last_ok"))
            if not last_ok or (ref - last_ok) > timedelta(hours=window_h):
                stats["stale"] += 1
                continue
        ok_rounds = int(entry.get("ok_rounds") or 0)
        if ok_rounds < min_ok:
            stats["unstable"] += 1
            continue
        kbps = float(entry.get("kbps") or 0)
        if min_kbps and kbps and kbps < min_kbps:
            stats["slow"] += 1
            continue
        proxy["_wl_kbps"] = kbps
        proxy["_wl_delay"] = int(entry.get("delay") or 0)
        proxy["_wl_rounds"] = ok_rounds
        # 存活起点：本机**首次测通**的时间（连续段）。账本把连续失败的条目标死并清掉，
        # 节点重新出现时 first_ok 重新写，所以它天然表示「这一段连续可用的历史」，
        # 不会把「死了三天又活过来」算成「活了三天」。
        first_ok = entry.get("first_ok")
        if first_ok and not entry.get("first_ok_est"):
            proxy["_wl_first_ok"] = first_ok
        else:
            estimated = estimate_age_hours(entry, cfg)
            if estimated:
                proxy["_age_est_h"] = estimated
        kept.append(proxy)

    kept.sort(key=_sort_key)
    return kept, stats


def strip(proxies):
    """剥掉白名单专用内部字段（不能写进订阅）。"""
    for proxy in proxies:
        for key in ("_wl_kbps", "_wl_delay", "_wl_rounds", "_wl_first_ok", "_age_est_h"):
            proxy.pop(key, None)
    return proxies


def load_candidates_bytes(raw):
    """解析候选池字节流（支持 .json 与 .json.gz）。"""
    import gzip
    import json

    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    data = json.loads(raw.decode("utf-8"))
    if isinstance(data, list):
        return data
    return data.get("proxies") or []


def load_candidates(path):
    """读本地候选池文件（本机探测用）。"""
    return load_candidates_bytes(path.read_bytes())
