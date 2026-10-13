"""中国大陆可达性检测：借助 Globalping 公开 API，从国内家宽探测点验证节点。

为什么不能只做 TCP 拨号（旧实现的问题）：
1. **CF/CDN 边缘假活**：节点挂在 Cloudflare 上（104.16/172.64/188.114 等 anycast IP），
   边缘对任何 443 握手都应答，源站/Worker 早就死了 TCP 照样通。只测 TCP 必然保留这种节点。
2. **探测点类型随机**：不带 tags 时可能分到国内机房探测点，机房国际出口与家宽不是一回事。
3. **单点表决**：一次探测遇到 probe 抖动就误判（实测同一节点连续两轮结果可以全相反）。
4. **SNI 才是被墙对象**：真实客户端发的是节点配置里的 SNI（sni/servername/ws-Host），
   对 IP 裸连得到的结论与客户端体验无关。

本实现的做法（与 mihomo 客户端行为对齐）：
- TLS 节点（http/tls、vmess/vless/trojan 的 tls）：从国内家宽探测点发真实 HTTPS 请求，
  SNI/Host/路径全部取节点配置；IP 类节点且有域名 SNI 时按 SNI 域名走（CDN anycast 等价）。
  TLS 握手完成就算网络层活着（纯代理端口常对普通 GET 回 503/直接断开，属正常）；
  仅当响应由 CDN 边缘（Cloudflare 等，响应头可识别）返回 5xx 时，才判定背后的
  源站/Worker 已死——边缘本身永远在线，5xx 才是节点真失效。
  ECONNRESET / 超时等明确网络错误记为不通票（GFW 对被墙 SNI 注入 RST 的典型表现）。
- reality（伪装握手，普通 TLS 必然失败）、明文 TCP 节点：退回 TCP 拨号。
- hysteria2/tuic（UDP/QUIC，API 测不了）：退回 ICMP，尽力而为。

配额与多轮表决（Globalping 免费额度 = 250 tests/小时，一个探测点算 1 test；
配置环境变量 GLOBALPING_TOKEN（免费注册 GitHub 账号）可提升到 500）：
- 第 1 轮每个单位 1 个家宽探测点，按网速从快到慢顺序测量（额度耗尽时先保住快节点）。
- 第 2、3 轮把剩余额度优先花在：①只有探测点报错（无结论）的单位；
  ②首轮通过、需要跨运营商复核的单位；③首轮不通、需要确认的单位。
  任何单位拿到 ≥2 票即定案；额度不足时单票也定案（探测点自身报错不计票）。
- 周期性插入百度对照组，探测网络异常时退避，避免把「探测点拥塞」误判成「节点不可达」。
- 429 按 Retry-After 退避重试；全部结束后对「未知」单位冷却补一轮，再不行才按未知处理。

两种模式（cfg.mode）：
- strict：国内验证通过才保留；不通/未知剔除（配额耗尽时顺序靠前的快节点先测）。
- safe：只剔除「国内不通但境外能通」的确认被墙节点，其余保留。
安全阀：若一个可达结果都没有、且几乎全是未知（限速/接口整体故障），跳过该过滤，
绝不因为第三方接口抖动把订阅写空。
"""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger("datiya.china")

API = "https://api.globalping.io/v1/measurements"
UA = "datiya-sub/1.0 (+https://github.com/WangMCweijia/datiya-sub)"

# 这些协议的端口是 UDP/QUIC，用 TCP/TLS 无法验证，只能退回 ICMP。
UDP_TYPES = {"hysteria", "hysteria2", "tuic"}


def _is_ip(host):
    parts = str(host or "").split(".")
    return len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts)


def _ws_host(proxy):
    headers = (proxy.get("ws-opts") or {}).get("headers") or {}
    return headers.get("Host") or headers.get("host")


def _sni_of(proxy):
    """复刻 mihomo 的 SNI 选择：显式 sni/servername > ws Host。"""
    return proxy.get("sni") or proxy.get("servername") or _ws_host(proxy)


def _is_tls(proxy):
    if proxy.get("type") == "trojan":
        return True
    return bool(proxy.get("tls"))


def _is_reality(proxy):
    # reality 握手是伪装协议，普通 TLS 请求必然失败，这类节点只能退回 TCP 拨号。
    return bool(proxy.get("reality-opts"))


# CDN 边缘特征头：这些平台对任何 SNI 都完成 TLS，5xx 只可能来自背后源站。
_CDN_MARKERS = ("server: cloudflare", "cf-ray", "x-amz-cf-id",
                "x-cache: error from cloudfront", "server: akamai", "x-served-by: cache-")


def _is_cdn_edge(result):
    """该 HTTP 结果是否由 CDN 边缘直接返回（看响应头特征）。"""
    blob = " ".join(str(result.get(k) or "").lower()
                    for k in ("rawHeaders", "rawOutput", "rawBody"))
    headers = result.get("headers") or {}
    if isinstance(headers, dict):
        blob += " " + " ".join(f"{k}: {v}" for k, v in headers.items()).lower()
    return any(marker in blob for marker in _CDN_MARKERS)


def _build_units(proxies):
    """把节点折叠成去重的探测单位，保持 proxies 的先后顺序（已按网速降序）。

    返回 (units, node_key)：units 为 [(key, unit)]（首次出现顺序，去重），
    node_key 把每个 proxy 对象映射到它所属单位的 key。
    """
    seen = set()
    units = []
    node_key = {}
    for proxy in proxies:
        ptype = str(proxy.get("type", "")).lower()
        server, port = proxy["server"], proxy.get("port", 443)

        if ptype in UDP_TYPES:
            key = ("icmp", server)
            unit = {"kind": "icmp", "target": server, "port": 0, "host": None, "path": None}
        elif _is_reality(proxy) or not _is_tls(proxy):
            key = ("tcp", server, port)
            unit = {"kind": "tcp", "target": server, "port": port, "host": None, "path": None}
        else:
            sni = _sni_of(proxy)
            host = _ws_host(proxy) or sni
            path = (proxy.get("ws-opts") or {}).get("path") or "/"
            # 有域名 SNI 就按 SNI 测：GFW 拦的是 SNI，CF 边缘按 SNI/Host 路由，
            # 客户端对「IP 节点」发出的 ClientHello 里写的也是这个 SNI。
            target = sni if (sni and not _is_ip(sni)) else server
            key = ("tls", target, port, host or "", path)
            unit = {"kind": "tls", "target": target, "port": port, "host": host, "path": path}

        node_key[id(proxy)] = key
        if key not in seen:
            seen.add(key)
            units.append((key, unit))
    return units, node_key


class _Globalping:
    def __init__(self, cfg):
        self.country = cfg.get("country", "CN")
        self.tag = cfg.get("tag", "eyeball-network")  # 只要真实家宽探测点
        self.concurrency = int(cfg.get("concurrency", 4))
        self.poll_interval = float(cfg.get("poll_interval", 2.0))
        self.timeout_s = float(cfg.get("timeout_s", 90))
        self.submit_gap = float(cfg.get("submit_gap", 0.35))
        self.control_every = int(cfg.get("control_every", 25))
        self.max_probes = int(cfg.get("max_probes", 3))
        # Globalping 免费额度：未认证 250 tests/h，带 token 500 tests/h，一个探测点 1 test。
        self.token = os.environ.get("GLOBALPING_TOKEN", "").strip()
        default_budget = 500 if self.token else 250
        self.hourly_budget = int(cfg.get("hourly_tests", default_budget))
        reserve = int(cfg.get("reserve_tests", 24))  # 给对照组/复核/冷却补测留的额度
        self.budget = max(10, self.hourly_budget - reserve)
        self.used = 0  # 已消耗 test 数（成功提交即计数）
        self._lock = threading.Lock()
        self._last_submit = 0.0
        self.session = requests.Session()
        ua = {"User-Agent": UA}
        if self.token:
            ua["Authorization"] = f"Bearer {self.token}"
        self.session.headers.update(ua)
        log.info("Globalping 额度预算：%d tests/h（已用额度 %d，token=%s），每单位最多 %d 点",
                 self.hourly_budget, self.used, "有" if self.token else "无", self.max_probes)

    def _throttle(self):
        with self._lock:
            wait = self.submit_gap - (time.monotonic() - self._last_submit)
            if wait > 0:
                time.sleep(wait)
            self._last_submit = time.monotonic()

    def _take_budget(self, n):
        """登记 n 个 test 的额度，不够则返回 False（额度按小时滚动，不够的单位按未知处理）。"""
        with self._lock:
            if self.used + n > self.budget:
                return False
            self.used += n
            return True

    def _payload(self, unit, country, tag, limit):
        loc = {"country": country}
        if tag:
            loc["tags"] = [tag]
        kind = unit["kind"]
        if kind in ("icmp", "tcp"):
            payload = {"type": "ping", "target": unit["target"],
                       "locations": [loc], "limit": limit}
            if kind == "tcp":
                payload["measurementOptions"] = {"protocol": "TCP", "port": unit["port"]}
            return payload
        headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
        if unit.get("host"):
            headers["Host"] = unit["host"]
        return {
            "type": "http",
            "target": unit["target"],
            "locations": [loc],
            "limit": limit,
            "measurementOptions": {
                "port": unit["port"],
                "protocol": "HTTPS",
                "request": {"method": "GET", "path": unit.get("path") or "/", "headers": headers},
            },
        }

    def submit(self, unit, country=None, tag=None, limit=1, count_budget=True, retries=7):
        """提交一次测量（默认 1 个探测点 = 1 test），成功返回 measurement id。

        额度不足返回 None（与请求非法/重试耗尽同口径，调用方记为无结论）。
        retries 控制 429/5xx 的退避重试次数（冷却补测轮调小，避免限速时长时间僵死）。
        """
        limit = int(limit)
        if count_budget and not self._take_budget(limit):
            log.debug("额度预算不足（%d/%d），跳过 %s", self.used, self.budget, unit["target"])
            return None
        payload = self._payload(unit, country or self.country,
                                tag if tag is not None else self.tag, limit)
        for attempt in range(max(1, retries)):
            self._throttle()
            try:
                resp = self.session.post(API, json=payload, timeout=20)
            except requests.RequestException:
                time.sleep(5 + 5 * attempt)
                continue
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = min(120, max(10, int(retry_after))) if retry_after else 20 + 20 * attempt
                except ValueError:
                    delay = 20 + 20 * attempt
                log.debug("429 限速，%s 等待 %ss（第 %d 次）", unit["target"], delay, attempt + 1)
                time.sleep(delay)
                continue
            if resp.status_code in (200, 202):
                return resp.json().get("id")
            log.debug("提交探测异常 %s: HTTP %s %s", unit["target"], resp.status_code,
                      resp.text[:120])
            if 400 <= resp.status_code < 500:
                return None  # 请求本身非法（如目标主机名不合法），重试无意义
            time.sleep(5 + 5 * attempt)
        return None

    def wait(self, measurement_id, kind):
        """返回各探测点判定列表：ok/dead/fail/err -> 票数统计在外部做。"""
        deadline = time.time() + self.timeout_s
        while time.time() < deadline:
            time.sleep(self.poll_interval)
            try:
                data = self.session.get(f"{API}/{measurement_id}", timeout=20).json()
            except (requests.RequestException, ValueError):
                continue
            if data.get("status") != "finished":
                continue
            verdicts = []
            for probe in data.get("results") or []:
                result = probe.get("result") or {}
                if kind in ("icmp", "tcp"):
                    stats = result.get("stats") or {}
                    if stats.get("avg") is not None:
                        verdicts.append("ok")
                    elif stats.get("total"):
                        verdicts.append("fail")
                    else:
                        verdicts.append("err")
                else:
                    code = result.get("statusCode")
                    timings = result.get("timings") or {}
                    if timings.get("tls"):
                        # TLS 能握上就说明网络层与证书/SNI 没被墙。
                        # 唯一例外：CDN 边缘（Cloudflare 等）回 5xx——边缘永远在线，
                        # 5xx 意味着背后的源站/Worker 已死，节点确实不可用。
                        # 非 CDN 的代理服务器对普通 GET 回 503/500 是正常现象
                        # （只服务 CONNECT），不能当死节点。
                        if code is not None and int(code) >= 500 and _is_cdn_edge(result):
                            verdicts.append("dead")
                        else:
                            verdicts.append("ok")
                    elif code is not None:
                        verdicts.append("fail")
                    elif result.get("rawOutput") or result.get("error"):
                        # 有明确错误结果：ECONNRESET（GFW 对 SNI 注入重置）、
                        # TCP/TLS 超时等，属于真实的网络层不通，算不通票。
                        verdicts.append("fail")
                    else:
                        # 什么结果都没有：探测点自身异常/排队丢失，不计票。
                        verdicts.append("err")
            return verdicts
        return None

    def measure(self, unit, country=None, tag=None, limit=1, count_budget=True, retries=7):
        mid = self.submit(unit, country, tag, limit, count_budget, retries)
        if not mid:
            return None
        return self.wait(mid, unit["kind"])

    def control_ok(self):
        """百度对照组：国内家宽 HTTPS 必须能完成，用于识别探测网络拥塞/故障窗口。"""
        unit = {"kind": "tls", "target": "www.baidu.com", "port": 443,
                "host": "www.baidu.com", "path": "/"}
        verdicts = self.measure(unit, limit=2)
        ok = bool(verdicts) and any(v == "ok" for v in verdicts)
        if not ok:
            log.warning("对照组 https://www.baidu.com 未通过（%s），探测网络可能拥塞，退避后重试",
                        verdicts)
        return ok


def _tally(verdicts):
    """3 状态：True=通过 / False=不通（含源站 5xx）/ None=样本不足。

    - ≥2 票一致即多数定案；
    - 只有 1 票且为明确结论（ok 或 fail/dead）时单票定案（免费额度下每单位往往只有 1 点）；
    - 探测点自身报错（err）不计票；1 通 1 不通这种分裂结果归未知，交下一轮补测。
    """
    if not verdicts:
        return None
    ok = sum(1 for v in verdicts if v == "ok")
    bad = sum(1 for v in verdicts if v in ("fail", "dead"))
    if ok >= 2:
        return True
    if bad >= 2:
        return False
    if ok and not bad:
        return True
    if bad and not ok:
        return False
    return None


def filter_reachable(proxies, cfg):
    """按中国大陆可达性过滤节点，返回保留的节点。"""
    if not proxies or not cfg or not cfg.get("enabled"):
        return proxies

    mode = str(cfg.get("mode", "safe")).lower()
    reference = cfg.get("reference_country", "US")
    gp = _Globalping(cfg)

    # proxies 已按网速降序；_build_units 保持该顺序，额度不够时先测/先保住快节点。
    units, node_key = _build_units(proxies)
    n_tls = sum(1 for _, u in units if u["kind"] == "tls")
    log.info(
        "中国可达性(%s)：从 %s 家宽探测点验证 %d 个单位（TLS %d、TCP %d、ICMP %d，对应 %d 节点），"
        "预算 %d tests、每单位最多 %d 点",
        mode, gp.country, len(units), n_tls,
        sum(1 for _, u in units if u["kind"] == "tcp"),
        sum(1 for _, u in units if u["kind"] == "icmp"),
        len(proxies), gp.hourly_budget, gp.max_probes,
    )

    votes = {key: [] for key, _ in units}
    lock = threading.Lock()
    state = {"seq": 0, "budget_hit": False}

    def run_round(targets):
        """对 targets 各追加 1 个探测点的票；对照组按序号周期性插入。"""
        def run(item):
            key, unit = item
            with lock:
                seq = state["seq"]
                state["seq"] += 1
            if gp.control_every and seq and seq % gp.control_every == 0:
                if not gp.control_ok():
                    time.sleep(45)
                    gp.control_ok()
            verdicts = gp.measure(unit)
            if verdicts is None:
                # 提交失败：额度预算耗尽（其它失败路径 submit 内部已退避重试）
                if gp.used >= gp.budget:
                    with lock:
                        state["budget_hit"] = True
                return key, ["err"]
            return key, verdicts[:1] or ["err"]

        with ThreadPoolExecutor(max_workers=gp.concurrency) as pool:
            for key, verdicts in pool.map(run, targets):
                votes[key].extend(verdicts)

    # 第 1 轮：每个单位 1 个家宽探测点
    run_round(units)
    log.info("中国可达性：第 1 轮完成（消耗 %d/%d tests）", gp.used, gp.budget)

    # 第 2..max 轮：剩余额度优先补给「无结论 → 单票通过 → 单票不通」的单位
    for rnd in range(1, gp.max_probes):
        if gp.used >= gp.budget:
            break
        groups = {None: [], True: [], False: []}
        for item in units:
            key = item[0]
            if len(votes[key]) > rnd:
                continue  # 这一轮已有票（理论上不会，防御）
            groups[_tally(votes[key])].append(item)
        targets = groups[None] + groups[True] + groups[False]
        # 只补还没凑满票数的单位
        targets = [it for it in targets if len(votes[it[0]]) < rnd + 1]
        if not targets:
            break
        run_round(targets)
        decided = sum(1 for k in votes if _tally(votes[k]) is not None)
        log.info("中国可达性：第 %d 轮完成（消耗 %d/%d tests，已定案 %d/%d）",
                 rnd + 1, gp.used, gp.budget, decided, len(units))
        if state["budget_hit"]:
            log.warning("中国可达性：额度预算耗尽，剩余单位按未知处理（严格模式剔除）")
            break

    # 冷却后对仍无结论的单位补最后一轮（滚动额度可能已释放；不再占用本地预算计数）
    unknown = [item for item in units if _tally(votes[item[0]]) is None]
    if unknown:
        log.info("中国可达性：%d 个单位仍无结论，冷却 45s 后补测", len(unknown))
        time.sleep(45)

        def run_cooldown(item):
            key, unit = item
            # 只试 2 次：真遇到持续 429 就放弃（按未知处理），不让构建拖太久
            return key, (gp.measure(unit, count_budget=False, retries=2) or ["err"])[:1]

        with ThreadPoolExecutor(max_workers=min(2, gp.concurrency)) as pool:
            for key, verdicts in pool.map(run_cooldown, unknown):
                if verdicts and verdicts != ["err"]:
                    votes[key].extend(verdicts)

    verdict_of_key = {key: _tally(votes[key]) for key, _ in units}
    reachable = {k for k, v in verdict_of_key.items() if v is True}
    unreachable = [k for k, v in verdict_of_key.items() if v is False]
    unk = [k for k, v in verdict_of_key.items() if v is None]
    multi = sum(1 for k, v in votes.items() if len(v) >= 2)
    log.info("中国可达性：票型分布 %d 单位多点、%d 单位单点（总消耗约 %d tests）",
             multi, len(units) - multi, gp.used)

    # 安全阀：一个通过结果都没有、且绝大多数未知 → 接口整体故障/全局限速，跳过过滤
    if units and not reachable and len(unk) >= max(1, int(len(units) * 0.8)):
        log.warning("中国可达性：%s 探测几乎全部未知（%d/%d），疑似限速或接口故障，跳过该过滤",
                    gp.country, len(unk), len(units))
        return proxies

    if mode == "strict":
        kept = [p for p in proxies if node_key[id(p)] in reachable]
        log.info(
            "中国可达性(strict)：可达 %d、不通 %d、未知 %d（单位口径）；剔除 %d/%d 个节点",
            len(reachable), len(unreachable), len(unk), len(proxies) - len(kept), len(proxies),
        )
        return kept

    # safe：只剔除「国内不通但境外能通」的确认被墙节点
    blocked = set()
    if unreachable and gp.used < gp.budget + gp.max_probes:
        log.info("中国可达性(safe)：%d 个单位国内不通，用 %s 探测点复核…",
                 len(unreachable), reference)
        unit_of_key = dict(units)

        def ref_probe(item):
            key, unit = item
            # 境外复核不做网络类型限制，1 点取明确结论即可
            return key, gp.measure(unit, country=reference, tag=False, limit=1)

        with ThreadPoolExecutor(max_workers=gp.concurrency) as pool:
            for key, verdicts in pool.map(ref_probe, [(k, unit_of_key[k]) for k in unreachable]):
                # 境外明确可完成 TLS、国内明确不通 → 确认被墙
                if verdicts and any(v == "ok" for v in verdicts):
                    blocked.add(key)

    kept = [p for p in proxies if node_key[id(p)] not in blocked]
    log.info("中国可达性(safe)：剔除 %d 个确认被墙节点，保留 %d/%d（未知 %d 个一律保留）",
             len(blocked), len(kept), len(proxies), len(unk))
    return kept
