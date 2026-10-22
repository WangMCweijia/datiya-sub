"""合并节点、生成固定订阅（Clash 配置 + v2ray 订阅）。"""

import base64
import hashlib
import json
import logging
import re
import unicodedata
from collections import defaultdict
from urllib.parse import quote

import yaml

log = logging.getLogger("datiya.build")

# 地区识别：subs 为子串匹配，codes 为独立词匹配（避免 us 命中 russia 这类误判）
REGION_RULES = [
    ("香港", ["香港", "hongkong", "hong kong", "hkg"], ["hk"]),
    ("台湾", ["台湾", "台灣", "taiwan"], ["tw"]),
    ("日本", ["日本", "japan"], ["jp"]),
    ("新加坡", ["新加坡", "狮城", "獅城", "singapore"], ["sg"]),
    ("韩国", ["韩国", "韓國", "korea"], ["kr"]),
    ("美国", ["美国", "美國", "united states", "america"], ["us", "usa"]),
    ("英国", ["英国", "英國", "united kingdom"], ["uk", "gb"]),
    ("德国", ["德国", "德國", "germany"], ["de"]),
    ("法国", ["法国", "法國", "france"], ["fr"]),
    ("荷兰", ["荷兰", "荷蘭", "netherlands"], ["nl"]),
    ("俄罗斯", ["俄罗斯", "俄羅斯", "russia"], ["ru"]),
    ("加拿大", ["加拿大", "canada"], ["ca"]),
    ("澳大利亚", ["澳大利亚", "澳洲", "australia"], ["au"]),
    ("印度", ["印度", "india"], ["in"]),
    ("土耳其", ["土耳其", "turkey"], ["tr"]),
    ("越南", ["越南", "vietnam"], ["vn"]),
    ("其他", [], []),
]

TOKEN_RE = re.compile(r"[a-z0-9]+")


def parse_proxies(text):
    """从一份 Clash 配置文本中提取 proxies 列表。"""
    try:
        data = yaml.safe_load(text)
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            return [p for p in data["proxies"] if isinstance(p, dict) and p.get("server")]
    except Exception as exc:
        log.warning("YAML 解析失败，改用逐行解析: %s", exc)
    return _parse_inline(text)


def _parse_inline(text):
    """兜底解析：只处理 proxies 段中 `- {k: v, ...}` 形式的行。"""
    result = []
    in_block = False
    for line in text.splitlines():
        if re.match(r"^proxies\s*:", line):
            in_block = True
            continue
        if not in_block:
            continue
        if line and not line[0].isspace():
            break
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        try:
            item = yaml.safe_load(stripped[2:])
        except Exception:
            continue
        if isinstance(item, dict) and item.get("server"):
            result.append(item)
    return result


def _ws_host(proxy):
    headers = (proxy.get("ws-opts") or {}).get("headers") or {}
    return headers.get("Host") or headers.get("host") or ""


def _credential(proxy, kind):
    """取节点的身份凭据：不同协议字段名不同，同一台机器的多份包装必须能对齐。

    明文代理必须用 `username:password` 组合：先按通用字段取 password 会把
    「同端口不同账号」当成同一节点（同一台 http 代理常挂多个账号）。
    """
    if kind in PLAINTEXT_TYPES:
        return f"{proxy.get('username') or ''}:{proxy.get('password') or ''}"
    for key in ("uuid", "password", "auth", "auth_str", "auth-str"):
        value = proxy.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def conn_key(proxy):
    """节点的**连接身份**（用于去重与跨构建追踪）。

    旧实现拿「除 name 外的全部字段」做 JSON key，上游只差 `udp: true`、
    `client-fingerprint`、`sub_tag` 这类附属字段就会被当成不同节点，于是同一台机器的
    同一端口在订阅里出现 5~6 份（实测 237 个节点只有 148 条线路）：既浪费测速与
    Globalping 额度，又让用户误以为「节点很多」，实际一死就是成片死。

    这里只保留决定「能不能连上、连到哪」的字段。
    """
    kind = str(proxy.get("type") or "").lower()
    server = str(proxy.get("server") or "").strip().lower()
    port = str(proxy.get("port") or "").strip()
    sni = proxy.get("sni") or proxy.get("servername") or _ws_host(proxy) or ""
    net = str(proxy.get("network") or "")
    path = (proxy.get("ws-opts") or {}).get("path") or (
        proxy.get("grpc-opts") or {}
    ).get("grpc-service-name") or ""
    return (kind, server, port, _credential(proxy, kind), str(sni), net, str(path))


def fingerprint(proxy):
    """连接身份的稳定短指纹，用作存活账本的键。"""
    return hashlib.sha1(
        "|".join(conn_key(proxy)).encode("utf-8", "replace")
    ).hexdigest()[:16]


_EMPTY = (None, "", [], {})


def dedupe(proxies):
    """按连接身份去重；参数相同的多份包装合并成一个节点（取字段最全的那份）。

    来源标记 `_srcs` 要**并集**而不是取第一个：跨源重复的节点知道「谁也给过它」，
    后面按源统计存活率时才不会把共享节点全算到第一个源头上。
    """
    order, best, merged_fields = [], {}, 0
    for proxy in proxies:
        key = conn_key(proxy)
        current = best.get(key)
        if current is None:
            best[key] = dict(proxy)
            best[key]["_srcs"] = list(proxy.get("_srcs") or [])
            order.append(key)
            continue
        mine = current.setdefault("_srcs", [])
        for name in proxy.get("_srcs") or []:
            if name not in mine:
                mine.append(name)
        for field, value in proxy.items():
            if field == "_srcs":
                continue
            if current.get(field) in _EMPTY and value not in _EMPTY:
                current[field] = value
                merged_fields += 1
    result = [best[key] for key in order]
    if merged_fields:
        log.info("去重时补全 %d 处缺失字段（同线路多份包装已合并）", merged_fields)
    return result


def classify(name):
    """根据节点名推测地区。"""
    lowered = str(name).lower()
    tokens = set(TOKEN_RE.findall(lowered))
    for region, subs, codes in REGION_RULES:
        if any(sub in lowered for sub in subs) or (codes and tokens & set(codes)):
            return region
    return "其他"


# 交给 mihomo 内核前先清洗：内核不认识的协议会让内核整体启动失败，必须剔除；
# 其余内核支持的协议一律保留，交给后面的测速环节去判断（不预先按类型误杀）。
#
# 明文代理（http / socks5）**保留**。2026-10-21 用 mihomo 内核对线上 237 个节点做
# 「每节点一个入站端口 → 真实 CONNECT 出网」实测：
#     http  107/119 = 89.9%   socks5 3/3 = 100%
#     vless  23/108 = 21.3%   hysteria2 0/4   trojan 0/2   ss 0/1
# 即 http 恰恰是最能用的那一类。此前「抽样 25 个仅 1 个可用（4%）」的结论是错的：
# 那是用 requests 直接拿 http://host:port 当代理测出来的，没有复用内核的
# 传输/认证/伪装逻辑，对该类节点有系统性低估。真正该做的是统一交给内核验证。
PLAINTEXT_TYPES = {"http", "socks5"}
CORE_TYPES = {
    "ss", "vmess", "vless", "trojan", "hysteria2", "hysteria", "anytls",
    "http", "socks5",
}
CORE_REQUIRED = {
    "ss": ("cipher", "password"),
    "vmess": ("uuid",),
    "vless": ("uuid",),
    "trojan": ("password",),
    "anytls": ("password",),
}
# 凭据字段有别名，任一存在即可
CORE_ANY = {
    "hysteria2": ("password", "auth"),
    "hysteria": ("auth_str", "auth-str"),
}


def _valid_core(proxy, kind):
    if not isinstance(proxy.get("server"), str) or not proxy["server"].strip():
        return False
    try:
        if not 1 <= int(proxy["port"]) <= 65535:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    if not all(proxy.get(f) not in (None, "") for f in CORE_REQUIRED.get(kind, ())):
        return False
    aliases = CORE_ANY.get(kind)
    if aliases and not any(proxy.get(f) not in (None, "") for f in aliases):
        return False
    return True


def filter_supported(proxies):
    """剔除内核不支持的协议，以及字段缺失/端口异常的节点。"""
    kept, dropped = [], {"不支持协议": 0, "字段异常": 0}
    for proxy in proxies:
        kind = str(proxy.get("type") or "").lower()
        if kind not in CORE_TYPES:
            dropped["不支持协议"] += 1
        elif not _valid_core(proxy, kind):
            dropped["字段异常"] += 1
        else:
            kept.append(proxy)
    detail = "，".join(f"{k} {v}" for k, v in dropped.items() if v)
    if detail:
        log.info("清洗掉 %d 个节点（%s）", len(proxies) - len(kept), detail)
    return kept


def _strip_controls(value):
    """递归清掉字符串里的控制字符（Unicode 类别 C*），其余原样保留。"""
    if isinstance(value, str):
        return "".join(ch for ch in value if unicodedata.category(ch)[0] != "C")
    if isinstance(value, dict):
        return {k: _strip_controls(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_controls(v) for v in value]
    return value


def sanitize(proxies):
    """清掉节点字段里的控制字符，并规范化个别会让 mihomo 拒绝整份配置的字段。

    上游源里偶有字段被错误解码（例如 emoji 被按 latin-1 读成乱码字节），落进 YAML 时会变成
    \\x9F 这类 C1 控制字符转义。mihomo / FlClash 的内核解析时会直接报
    "yaml: control characters are not allowed" 并拒绝**整个**配置，表现为客户端节点列表空白。

    另外，vmess 节点若带空字符串 cipher（security），mihomo 会报
    "unsupported security type: \"\"" 并拒绝**整份**测速配置（几千个节点里一个坏节点
    就会让真实测速整体回退）。这里统一归一成内核支持的取值，无法识别的用 auto。
    """
    # mihomo vmess 支持的加密方式；其余取值（含空串）一律退回 auto（由内核自行协商）。
    VMESS_SECURITY = {"auto", "none", "zero", "aes-128-gcm", "chacha20-poly1305"}
    fixed = 0
    vmess_fixed = 0
    for proxy in proxies:
        if str(proxy.get("type")).lower() == "vmess":
            raw = proxy.get("cipher")
            security = str(raw).strip().lower() if raw not in (None, "") else ""
            if security not in VMESS_SECURITY:
                proxy["cipher"] = "auto"
                vmess_fixed += 1
        for key, value in list(proxy.items()):
            cleaned = _strip_controls(value)
            if cleaned != value:
                fixed += 1
                proxy[key] = cleaned
    if fixed:
        log.info("清洗掉 %d 处含控制字符的字段", fixed)
    if vmess_fixed:
        log.info("规范化 %d 个 vmess 节点的加密方式为 auto", vmess_fixed)
    return proxies


def _unique_names(proxies):
    """保证输出配置里的节点名唯一。

    注意：必须按「最终生成的名字」判重，而不是按基础名计数。否则当某个源站节点名
    本身就带 " #2" 时，会和另一个同基础名节点被改出的 "xxx #2" 撞车，导致配置里出现
    重名——mihomo 遇到重名会直接拒绝启动，客户端一个节点都加载不出来。
    """
    used = set()
    for proxy in proxies:
        base = str(proxy.get("name") or "node").strip() or "node"
        name = base
        n = 1
        while name in used:
            n += 1
            name = f"{base} #{n}"
        used.add(name)
        proxy["name"] = name
    return proxies


def rank_by_protocol(proxies, scores, high_ratio=0.75, cap=None, source_scores=None):
    """按协议「实测真实出网可用率」分区排序，让最能用的节点先占探测额度。

    依据 2026-10-21 在本机（国内家宽）用 mihomo 内核逐节点真实 CONNECT 的实测：
        socks5 100%    http 84~90%    vless 10~21%    ss/vmess/trojan/anytls/hy2 ≈ 0%
    协议差异极大，而 Globalping 的探测名额（`cap`，默认 240）是稀缺资源：
    让 0% 可用的协议去占名额，等于把额度浪费在必死节点上。

    高可用协议（≥80 分）最多占 `high_ratio × cap` 个名额，余量留给其他协议，
    避免订阅被单一协议垄断——同源节点往往成批失效，需要留点冗余。

    注意 `cap` 必须传「探测名额」而不是候选总数：候选可能有几千个，
    拿它算配额会让配额永远大于实际名额，等于配比彻底失效（踩过一次）。

    源倾斜（`source_scores`，0~100 表示该源实测存活率的相对值）：2026-10-22 按源实测
    daily_free_vpn 38%、Au1rxx-07 17.5%、ermaozi 15%、Au1rxx-06 10%、TopFreeProxy 0/40。
    协议只能解释一半方差 —— 「http 来自 daily_free_vpn」和「http 来自 TopFreeProxy」
    的命运完全不同。所以分区仍按**协议原始分**（不乘源因子：否则某个主力源临时 404
    就会把其他源的高可用节点一起打到低分区，白白饿死备份），分区内则按
    `协议分 × 源因子` 排优先级，额度不够时先砍掉低信誉源的节点。
    """
    if not scores:
        return proxies
    weight = {str(k).lower(): float(v) for k, v in scores.items()}
    src_weight = {str(k).lower(): float(v) / 100.0 for k, v in (source_scores or {}).items()}

    def score(p):
        return weight.get(str(p.get("type") or "").lower(), 0.0)

    def src_factor(p):
        if not src_weight:
            return 1.0
        names = [str(n).lower() for n in (p.get("_srcs") or [])]
        # 多个源都给过这个节点时按最好的那个算（它至少有一个源在供血）
        return max([src_weight.get(n, 0.5) for n in names] or [0.5])

    def priority(p):
        return -score(p) * src_factor(p)

    high = [p for p in proxies if score(p) >= 80]
    low = [p for p in proxies if score(p) < 80]
    # 分区内按「协议分 × 源因子」排（sort 稳定，同分保持上游的存活次数/网速序）
    high.sort(key=priority)
    low.sort(key=priority)
    quota = max(1, int((cap or len(proxies)) * float(high_ratio)))
    if len(high) <= quota:
        return high + low
    # 超出配额的高分节点排到最后，额度不够时优先被截掉
    return high[:quota] + low + high[quota:]



def diversify(proxies, per_line=2, trusted_streak=3, trusted_cap=4):
    """同一 `server:port` 最多保留 per_line 个凭据，提升订阅的线路多样性。

    公开源里同一台机器的「不同账号」常常成批出现（同一 IP:端口挂几十个 uuid），
    实测发布订阅里 237 个节点只对应 148 条线路，最夸张的一条线路占 6 份。
    这些多份凭据往往同时失效——留多份只是重复占位、挤占探测额度，
    且让用户误以为节点很多。存活达标的节点放宽到 trusted_cap，避免误伤真正稳的。
    """
    if not per_line or per_line <= 0:
        return proxies
    used = {}
    kept, dropped = [], 0
    for proxy in proxies:
        key = (str(proxy.get("server")), proxy.get("port"))
        count = used.get(key, 0)
        cap = trusted_cap if int(proxy.get("_streak") or 0) >= trusted_streak else per_line
        if count >= cap:
            dropped += 1
            continue
        used[key] = count + 1
        kept.append(proxy)
    if dropped:
        log.info("线路去冗：同一 server:port 最多保留 %d 个凭据（存活达标放宽到 %d），剔除 %d 个",
                 per_line, trusted_cap, dropped)
    return kept


def build_clash(proxies, clash_cfg, meta):
    """生成完整、可直接导入的 Clash 配置。"""
    # 防御：_speed_kbps / _streak 这类内部字段一旦写进配置，mihomo 会拒绝整份文件
    # （客户端表现为节点列表空白），这里再兜一层，不依赖调用方先剥干净。
    clean = [
        {k: v for k, v in proxy.items() if not k.startswith("_")} for proxy in proxies
    ]
    names = [p["name"] for p in clean]
    test_url = clash_cfg.get("test_url", "http://www.gstatic.com/generate_204")
    interval = int(clash_cfg.get("test_interval", 300))
    # url-test 默认 5 秒判超时，晚高峰国际链路 TTFB 常到 5~8 秒，会把其实能用的节点误判为 timeout。
    test_timeout = int(clash_cfg.get("test_timeout", 8000))

    by_region = defaultdict(list)
    for proxy in clean:
        by_region[classify(proxy["name"])].append(proxy["name"])

    region_groups = [
        {
            "name": f"🌐 {region}",
            "type": "url-test",
            "url": test_url,
            "interval": interval,
            "tolerance": 50,
            "timeout": test_timeout,
            "proxies": nodes,
        }
        for region, nodes in sorted(by_region.items(), key=lambda kv: (kv[0] == "其他", kv[0]))
    ]

    groups = [
        {
            "name": "🚀 节点选择",
            "type": "select",
            "proxies": ["♻️ 自动选择", "DIRECT"] + [g["name"] for g in region_groups] + names,
        },
        {
            "name": "♻️ 自动选择",
            "type": "url-test",
            "url": test_url,
            "interval": interval,
            "tolerance": 50,
            "timeout": test_timeout,
            "proxies": names,
        },
    ]
    groups.extend(region_groups)

    config = {
        "port": int(clash_cfg.get("http_port", 7890)),
        "socks-port": int(clash_cfg.get("socks_port", 7891)),
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "external-controller": clash_cfg.get("external_controller", "127.0.0.1:9090"),
        "dns": _dns_config(),
        "proxies": clean,
        "proxy-groups": groups,
        "rules": _rules(),
    }
    header = (
        "# 由 datiya-sub 自动生成，请勿手动修改\n"
        f"# 更新于 {meta['updated_at']} | 可用节点 {meta['alive']} 个"
        f"（候选 {meta['total']} 个，datiya 覆盖 {meta['days']} 天）\n"
    )
    body = yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
        width=4096,
    )
    return header + body


def _dns_config():
    return {
        "enable": True,
        "ipv6": False,
        "enhanced-mode": "fake-ip",
        "fake-ip-range": "198.18.0.1/16",
        "fake-ip-filter": ["*.lan", "*.local", "localhost.ptlogin2.qq.com"],
        "nameserver": ["223.5.5.5", "119.29.29.29"],
        "fallback": ["https://1.1.1.1/dns-query", "https://dns.google/dns-query"],
    }


def _rules():
    return [
        "IP-CIDR,127.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,10.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,172.16.0.0/12,DIRECT,no-resolve",
        "IP-CIDR,192.168.0.0/16,DIRECT,no-resolve",
        "IP-CIDR,100.64.0.0/10,DIRECT,no-resolve",
        "DOMAIN-SUFFIX,cn,DIRECT",
        "GEOIP,CN,DIRECT",
        "MATCH,🚀 节点选择",
    ]


SUPPORTED_LINK_TYPES = ("ss", "vmess", "trojan", "vless", "hysteria2", "anytls")


def build_links(proxies):
    """生成 v2rayN / v2rayNG / NekoBox 等客户端可用的分享链接。"""
    links = []
    for proxy in proxies:
        kind = str(proxy.get("type") or "").lower()
        if kind not in SUPPORTED_LINK_TYPES:
            continue
        try:
            links.append(_make_link(proxy))
        except Exception as exc:
            log.debug("节点 %s 转分享链接失败: %s", proxy.get("name"), exc)
    return links


def b64(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _transport(proxy):
    """把 mihomo 的传输层参数翻译成分享链接的 query 片段。

    reality / gRPC 这类节点的参数字段名与分享链接完全不同，漏一个客户端就废：
    实测旧实现给 17 个 reality 节点生成的 vless 链接里 pbk/sid/fp 全部缺失，
    导入 v2rayN 必然连不上（reality 没有 publicKey 无法握手）。
    """
    query = []
    net = str(proxy.get("network") or "tcp")
    ws_opts = proxy.get("ws-opts") or {}
    grpc_opts = proxy.get("grpc-opts") or {}
    if net == "ws":
        host = _ws_host(proxy)
        if host:
            query.append(("host", host))
        path = ws_opts.get("path")
        if path:
            query.append(("path", path))
    elif net == "grpc":
        service = grpc_opts.get("grpc-service-name")
        if service:
            query.append(("serviceName", service))
    elif net in ("h2", "http"):
        hosts = (proxy.get("h2-opts") or {}).get("host") or []
        if hosts:
            query.append(("host", hosts[0] if isinstance(hosts, list) else hosts))
        path = (proxy.get("h2-opts") or {}).get("path")
        if path:
            query.append(("path", path))
    return query


def _make_link(proxy):
    kind = str(proxy.get("type") or "").lower()
    name = str(proxy.get("name"))
    server = str(proxy.get("server"))
    port = str(proxy.get("port"))
    tag = "#" + _quote(name)

    if kind == "ss":
        userinfo = b64(f"{proxy.get('cipher', 'aes-256-gcm')}:{proxy.get('password', '')}")
        query = []
        plugin = proxy.get("plugin")
        if plugin:
            opts = proxy.get("plugin-opts") or {}
            value = f"{plugin};{_plugin_opts(opts)}" if opts else str(plugin)
            query.append(("plugin", value))
        tail = f"?{_query(query)}" if query else ""
        return f"ss://{userinfo}@{server}:{port}{tail}{tag}"

    if kind == "vmess":
        net = str(proxy.get("network", "tcp"))
        service = (proxy.get("grpc-opts") or {}).get("grpc-service-name", "")
        payload = {
            "v": "2",
            "ps": name,
            "add": server,
            "port": port,
            "id": str(proxy.get("uuid", "")),
            "aid": str(proxy.get("alterId", 0)),
            "scy": str(proxy.get("cipher") or "auto"),
            "net": net,
            "type": "none",
            "host": _ws_host(proxy) or str(proxy.get("servername") or ""),
            "path": str((proxy.get("ws-opts") or {}).get("path") or service or ""),
            "tls": "tls" if proxy.get("tls") else "",
            "sni": str(proxy.get("servername") or proxy.get("sni") or ""),
            "fp": str(proxy.get("client-fingerprint") or ""),
        }
        return "vmess://" + base64.b64encode(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        ).decode()

    if kind == "trojan":
        query = [("security", "tls"), ("type", str(proxy.get("network", "tcp")))]
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("allowInsecure", "1"))
        query += _transport(proxy)
        return f"trojan://{_quote(str(proxy.get('password', '')))}@{server}:{port}?{_query(query)}{tag}"

    if kind == "vless":
        reality = proxy.get("reality-opts") or {}
        if reality:
            security = "reality"
        elif proxy.get("tls"):
            security = "tls"
        else:
            security = "none"
        query = [
            ("encryption", "none"),
            ("security", security),
            ("type", str(proxy.get("network", "tcp"))),
        ]
        sni = proxy.get("servername") or proxy.get("sni")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("allowInsecure", "1"))
        if proxy.get("flow"):
            query.append(("flow", str(proxy["flow"])))
        if reality:
            # reality 三件套：publicKey 必需，shortId/SpiderX 有则带上；
            # fingerprint 也必需（reality 握手靠它伪装浏览器）。
            if reality.get("public-key"):
                query.append(("pbk", str(reality["public-key"])))
            if reality.get("short-id"):
                query.append(("sid", str(reality["short-id"])))
            if reality.get("spider-x"):
                query.append(("spx", str(reality["spider-x"])))
            query.append(("fp", str(proxy.get("client-fingerprint") or "chrome")))
        elif proxy.get("client-fingerprint"):
            query.append(("fp", str(proxy["client-fingerprint"])))
        query += _transport(proxy)
        return f"vless://{proxy.get('uuid', '')}@{server}:{port}?{_query(query)}{tag}"

    if kind == "hysteria2":
        query = []
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("insecure", "1"))
        obfs = proxy.get("obfs")
        if obfs:
            query.append(("obfs", str(obfs)))
            if proxy.get("obfs-password"):
                query.append(("obfs-password", str(proxy["obfs-password"])))
        password = proxy.get("password") or proxy.get("auth") or ""
        tail = f"?{_query(query)}" if query else ""
        return f"hysteria2://{_quote(str(password))}@{server}:{port}{tail}{tag}"

    if kind == "anytls":
        query = []
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("insecure", "1"))
        if proxy.get("client-fingerprint"):
            query.append(("fp", str(proxy["client-fingerprint"])))
        tail = f"?{_query(query)}" if query else ""
        return f"anytls://{_quote(str(proxy.get('password', '')))}@{server}:{port}{tail}{tag}"

    raise ValueError(f"不支持的协议: {kind}")


def _plugin_opts(opts):
    """ss 插件参数拼成 SIP002 的 `plugin` 值（k=v 用分号连接）。"""
    pairs = []
    for key in ("mode", "host", "path", "tls"):
        value = opts.get(key)
        if value in (None, "", False):
            continue
        if value is True:
            pairs.append(str(key))
        else:
            pairs.append(f"{key}={value}")
    return ";".join(pairs)


def _query(pairs):
    return "&".join(f"{k}={_quote(str(v))}" for k, v in pairs)


def _quote(value):
    return quote(value, safe="")