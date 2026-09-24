"""合并节点、生成固定订阅（Clash 配置 + v2ray 订阅）。"""

import base64
import json
import logging
import re
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


def dedupe(proxies):
    """按连接参数去重（名称不同但参数相同的视为同一节点）。"""
    seen = set()
    result = []
    for proxy in proxies:
        key = json.dumps(
            {k: v for k, v in proxy.items() if k != "name"},
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(proxy)
    return result


def classify(name):
    """根据节点名推测地区。"""
    lowered = str(name).lower()
    tokens = set(TOKEN_RE.findall(lowered))
    for region, subs, codes in REGION_RULES:
        if any(sub in lowered for sub in subs) or (codes and tokens & set(codes)):
            return region
    return "其他"


def _unique_names(proxies):
    """保证输出配置里的节点名唯一。"""
    used = {}
    for proxy in proxies:
        base = str(proxy.get("name") or "node").strip() or "node"
        count = used.get(base, 0) + 1
        used[base] = count
        proxy["name"] = base if count == 1 else f"{base} #{count}"
    return proxies


def build_clash(proxies, clash_cfg, meta):
    """生成完整、可直接导入的 Clash 配置。"""
    names = [p["name"] for p in proxies]
    test_url = clash_cfg.get("test_url", "http://www.gstatic.com/generate_204")
    interval = int(clash_cfg.get("test_interval", 300))

    by_region = defaultdict(list)
    for proxy in proxies:
        by_region[classify(proxy["name"])].append(proxy["name"])

    region_groups = [
        {
            "name": f"🌐 {region}",
            "type": "url-test",
            "url": test_url,
            "interval": interval,
            "tolerance": 50,
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
        "proxies": proxies,
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


SUPPORTED_LINK_TYPES = ("ss", "vmess", "trojan", "vless")


def build_links(proxies):
    """生成 v2rayN / v2rayNG 等客户端可用的分享链接。"""
    links = []
    for proxy in proxies:
        kind = proxy.get("type")
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


def _make_link(proxy):
    kind = proxy["type"]
    name = str(proxy["name"])
    server = str(proxy["server"])
    port = str(proxy["port"])
    tag = "#" + _quote(name)

    if kind == "ss":
        userinfo = b64(f"{proxy.get('cipher', 'aes-256-gcm')}:{proxy.get('password', '')}")
        return f"ss://{userinfo}@{server}:{port}{tag}"

    if kind == "vmess":
        payload = {
            "v": "2",
            "ps": name,
            "add": server,
            "port": port,
            "id": str(proxy.get("uuid", "")),
            "aid": str(proxy.get("alterId", 0)),
            "scy": str(proxy.get("cipher", "auto")),
            "net": str(proxy.get("network", "tcp")),
            "type": "none",
            "host": str(proxy.get("ws-opts", {}).get("headers", {}).get("Host", "") or proxy.get("servername", "")),
            "path": str(proxy.get("ws-opts", {}).get("path", "")),
            "tls": "tls" if proxy.get("tls") else "",
            "sni": str(proxy.get("servername", "") or proxy.get("sni", "")),
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
        return f"trojan://{_quote(str(proxy.get('password', '')))}@{server}:{port}?{_query(query)}{tag}"

    if kind == "vless":
        query = [
            ("encryption", "none"),
            ("security", "tls" if proxy.get("tls") else "none"),
            ("type", str(proxy.get("network", "tcp"))),
        ]
        sni = proxy.get("servername") or proxy.get("sni")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("allowInsecure", "1"))
        if proxy.get("flow"):
            query.append(("flow", str(proxy["flow"])))
        return f"vless://{proxy.get('uuid', '')}@{server}:{port}?{_query(query)}{tag}"

    raise ValueError(f"不支持的协议: {kind}")


def _query(pairs):
    return "&".join(f"{k}={_quote(str(v))}" for k, v in pairs)


def _quote(value):
    return quote(value, safe="")