"""中国大陆可达性检测：借助 Globalping 公开 API，用中国大陆探测点 ping 节点服务器。

动机：GitHub Actions 的测活机在境外，「境外连得通」不等于「国内连得通」。
mihomo 两轮测速只能验证节点在境外可用，这里再补一刀：把**确认在国内被墙**的服务器
从订阅里剔除。

判定规则（关键：只剔除有证据的，绝不误杀）：
- 从中国大陆探测点能 ping 通            → 保留
- 从中国大陆 ping 不通，但换一个境外探测点能 ping 通 → 确认被墙，剔除
- 两边都 ping 不通                       → 多半是该服务器屏蔽了 ICMP，无法据此判断，
                                           一律保留（避免把能用的节点误杀）

实现要点：
- 只 ping 服务器地址，不验证端口/协议，属「必要不充分」条件。
- Globalping 免费额度约 250 次/小时，故默认每步只用 1 个探测点，并按 server 去重减少次数。
- 任何异常（限速 429、网络错误、超时）都记为「未知」，未知一律保留，
  绝不因为第三方接口抖动把订阅写空。
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger("datiya.china")

API = "https://api.globalping.io/v1/measurements"
UA = "datiya-sub/1.0 (+https://github.com/WangMCweijia/datiya-sub)"


def _ping(host, country, limit, poll_interval, timeout_s):
    """从指定国家探测点 ping 一个主机，返回 True(可达)/False(不可达)/None(未知)。"""
    try:
        resp = requests.post(
            API,
            json={
                "type": "ping",
                "target": host,
                "locations": [{"country": country}],
                "limit": limit,
            },
            headers={"User-Agent": UA},
            timeout=20,
        )
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
        # 任意一个探测点收到 ICMP 响应即视为可达
        for probe in data.get("results") or []:
            stats = (probe.get("result") or {}).get("stats") or {}
            if stats.get("avg") is not None:
                return True
        return False
    return None


def filter_reachable(proxies, cfg):
    """剔除确认在国内被墙的服务器，返回保留的节点。"""
    if not proxies or not cfg or not cfg.get("enabled"):
        return proxies

    country = cfg.get("country", "CN")
    reference = cfg.get("reference_country", "US")
    limit = int(cfg.get("limit", 1))
    concurrency = int(cfg.get("concurrency", 8))
    poll_interval = float(cfg.get("poll_interval", 2.0))
    timeout_s = float(cfg.get("timeout_s", 90))

    servers = sorted({p["server"] for p in proxies})
    log.info("中国可达性：从 %s 探测 %d 个服务器（对应 %d 个节点）", country, len(servers), len(proxies))

    def probe(host, where):
        return host, _ping(host, where, limit, poll_interval, timeout_s)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        cn = dict(pool.map(lambda h: probe(h, country), servers))

    unknown_cn = sum(1 for v in cn.values() if v is None)
    if servers and unknown_cn == len(servers):
        log.warning("中国可达性：%s 探测全部未取到结果（可能被限速或接口异常），跳过该过滤", country)
        return proxies

    suspects = [h for h in servers if cn[h] is False]
    log.info(
        "中国可达性：%s 可达 %d 个，不通 %d 个；再用 %s 探测点复核是否只是屏蔽了 ICMP",
        country,
        sum(1 for v in cn.values() if v is True),
        len(suspects),
        reference,
    )

    blocked = set()
    unknown_ref = 0
    if suspects:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            ref = dict(pool.map(lambda h: probe(h, reference), suspects))
        unknown_ref = sum(1 for h in suspects if ref.get(h) is None)
        # 境外能通、国内不通 → 确认被墙
        blocked = {h for h in suspects if ref.get(h) is True}

    kept = [p for p in proxies if p["server"] not in blocked]
    log.info(
        "中国可达性：剔除 %d 个确认被墙的节点，保留 %d/%d 个"
        "（其中 %d 个服务器屏蔽 ICMP 无法判断、%d 个复核结果未知，均已保留）",
        len(proxies) - len(kept),
        len(kept),
        len(proxies),
        len(suspects) - len(blocked) - unknown_ref,
        unknown_ref,
    )
    return kept
