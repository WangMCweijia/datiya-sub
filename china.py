"""中国大陆可达性检测：借助 Globalping 公开 API，从中国大陆探测点检查节点的代理端口。

动机：GitHub Actions 的测活机在境外，「境外连得通」不等于「国内连得通」。
而且只 ping 主机（ICMP）是「必要不充分」的——很多运营商放行 ICMP，却封掉代理的真实端口，
于是「ping 通、端口不通」的节点被保留，用户导入后一测就是失效。
所以对 TCP 协议直接对 host:port 做 TCP 拨号；hysteria2 这类 UDP 协议没法用 TCP 验证，
只能退回 ICMP（属于尽力而为）。

两种模式（cfg.mode）：
- strict（激进）：国内连得上 → 保留；连不上或结果未知 → 剔除。
  节点更少但更干净，代价是可能误杀。
- safe（保守）：只剔除「国内不通但境外能通」的确认被墙节点；
  国内不通、境外也不通的多半是探测协议不匹配，无法判断，一律保留（几乎不误杀）。

实现要点：
- Globalping 免费额度有限（约 250 次/小时），故默认每步只用 1 个探测点，并按 (server, port) 去重。
- 安全阀：若国内探测结果全部未知（限速 429、接口异常、网络错误），说明拿不到有效信号，
  直接跳过该过滤，绝不因为第三方接口抖动把订阅写空。
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger("datiya.china")

API = "https://api.globalping.io/v1/measurements"
UA = "datiya-sub/1.0 (+https://github.com/WangMCweijia/datiya-sub)"

# 这些协议的端口是 UDP/QUIC，用 TCP 拨号必然失败，只能退回 ICMP 判断。
UDP_TYPES = {"hysteria", "hysteria2", "tuic"}


def _probe(host, port, proto, country, limit, poll_interval, timeout_s):
    """从指定国家的探测点检查可达性，返回 True(可达)/False(不可达)/None(未知)。

    proto="tcp"：对 host:port 做 TCP 拨号，能直接反映「这个代理端口国内连不连得上」；
    proto="icmp"：只 ping 主机（用于 UDP 协议，端口无法用 TCP 验证）。
    """
    payload = {
        "type": "ping",
        "target": host,
        "locations": [{"country": country}],
        "limit": limit,
    }
    if proto == "tcp":
        payload["measurementOptions"] = {"protocol": "TCP", "port": port}

    try:
        resp = requests.post(API, json=payload, headers={"User-Agent": UA}, timeout=20)
    except requests.RequestException as exc:
        log.debug("china: 提交探测失败 %s: %s", host, exc)
        return None

    if resp.status_code == 429:
        log.debug("china: 触发限速，%s 记为未知", host)
        return None
    if not (200 <= resp.status_code < 300):
        log.debug("china: 提交探测异常 %s: HTTP %s", host, resp.status_code)
        return None

    try:
        measurement_id = resp.json().get("id")
    except ValueError:
        return None
    if not measurement_id:
        return None

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            data = requests.get(
                f"{API}/{measurement_id}", headers={"User-Agent": UA}, timeout=20
            ).json()
        except (requests.RequestException, ValueError):
            continue
        if data.get("status") != "finished":
            continue
        # 任意一个探测点拿到往返时间即视为可达
        for probe in data.get("results") or []:
            stats = (probe.get("result") or {}).get("stats") or {}
            if stats.get("avg") is not None:
                return True
        return False
    return None


def _protocol_of(types):
    """该端口的探测方式：只要有一个 TCP 协议在用，就做 TCP 拨号。"""
    kinds = {str(t).lower() for t in types if t}
    return "icmp" if kinds and kinds <= UDP_TYPES else "tcp"


def filter_reachable(proxies, cfg):
    """按中国大陆可达性过滤节点，返回保留的节点。"""
    if not proxies or not cfg or not cfg.get("enabled"):
        return proxies

    mode = str(cfg.get("mode", "safe")).lower()
    country = cfg.get("country", "CN")
    reference = cfg.get("reference_country", "US")
    limit = int(cfg.get("limit", 1))
    concurrency = int(cfg.get("concurrency", 8))
    poll_interval = float(cfg.get("poll_interval", 2.0))
    timeout_s = float(cfg.get("timeout_s", 90))

    # 端口也是关键：同一个服务器换个端口可能就通/不通，所以按 (server, port) 去重。
    endpoints = {}
    for proxy in proxies:
        endpoints.setdefault((proxy["server"], proxy.get("port")), set()).add(proxy.get("type"))

    pairs = sorted(endpoints)
    tcp_count = sum(1 for key in pairs if _protocol_of(endpoints[key]) == "tcp")
    log.info(
        "中国可达性(%s)：从 %s 探测 %d 个「服务器:端口」（TCP 拨号 %d 个、ICMP %d 个，对应 %d 个节点）",
        mode,
        country,
        len(pairs),
        tcp_count,
        len(pairs) - tcp_count,
        len(proxies),
    )

    def probe(key, where):
        host, port = key
        proto = _protocol_of(endpoints[key])
        return key, _probe(host, port, proto, where, limit, poll_interval, timeout_s)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        cn = dict(pool.map(lambda key: probe(key, country), pairs))

    unknown_cn = sum(1 for v in cn.values() if v is None)
    if pairs and unknown_cn == len(pairs):
        log.warning("中国可达性：%s 探测全部未取到结果（可能被限速或接口异常），跳过该过滤", country)
        return proxies

    reachable = {key for key, value in cn.items() if value is True}
    unreachable = [key for key in pairs if cn[key] is False]
    unknown = [key for key in pairs if cn[key] is None]

    if mode == "strict":
        # 激进：只保留国内连得上的；连不上或未知一律剔除。
        kept = [p for p in proxies if (p["server"], p.get("port")) in reachable]
        log.info(
            "中国可达性(strict)：%s 可达 %d 个、不通 %d 个、未知 %d 个；"
            "只保留可达的，剔除 %d/%d 个节点",
            country,
            len(reachable),
            len(unreachable),
            len(unknown),
            len(proxies) - len(kept),
            len(proxies),
        )
        return kept

    # safe：只剔除「国内不通但境外能通」的确认被墙节点，其余（含未知）一律保留。
    log.info(
        "中国可达性(safe)：%s 可达 %d 个，不通 %d 个；再用 %s 探测点复核是否只是探测协议不匹配",
        country,
        len(reachable),
        len(unreachable),
        reference,
    )

    blocked = set()
    unknown_ref = 0
    if unreachable:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            ref = dict(pool.map(lambda key: probe(key, reference), unreachable))
        unknown_ref = sum(1 for key in unreachable if ref.get(key) is None)
        # 境外能通、国内不通 → 确认被墙
        blocked = {key for key in unreachable if ref.get(key) is True}

    kept = [p for p in proxies if (p["server"], p.get("port")) not in blocked]
    log.info(
        "中国可达性(safe)：剔除 %d 个确认被墙的节点，保留 %d/%d 个"
        "（其中 %d 个服务器:端口无法判断、%d 个复核结果未知，均已保留）",
        len(proxies) - len(kept),
        len(kept),
        len(proxies),
        len(unreachable) - len(blocked) - unknown_ref,
        unknown_ref,
    )
    return kept