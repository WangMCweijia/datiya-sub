"""彻查实验3（runner）：真 SNI 的 TLS 层矩阵 × mihomo 端到端存活交叉表。

- 用每个节点真实配置的 SNI / ws-Host 发 HTTPS 请求（CF 节点按 SNI 域名走 anycast）；
- CN 家宽 3 点表决，未过的再用 US 2 点区分「被墙」与「源站死」；
- 并发压到 3、每 12 个单位插一次百度对照，防止把 probe 拥塞误判成不可达；
- 与 runner 侧 mihomo 真实下载测速结果（境外端到端）做逐节点交叉。
输出 audit3.csv + 控制台汇总。
"""
import csv
import logging
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import requests
import yaml

import prober

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s", datefmt="%H:%M:%S")

API = "https://api.globalping.io/v1/measurements"
H = {"User-Agent": "audit3/1.0"}
SUB = "https://wangmcweijia.github.io/datiya-sub/sub/clash.yaml"

# ---------- 1. 线上订阅 + 境外 mihomo 端到端重测 ----------
r = requests.get(SUB, timeout=40)
r.encoding = "utf-8"
proxies = yaml.safe_load(r.text)["proxies"]
print(f"线上订阅 {len(proxies)} 节点", flush=True)

cfg = yaml.safe_load(open("config.yaml"))["check"]
alive_list = prober.filter_working(proxies, cfg)
alive = {p["name"] for p in alive_list}
print(f"mihomo 境外端到端存活 {len(alive)}/{len(proxies)}", flush=True)


def is_domain(host):
    parts = str(host).split(".")
    return not (len(parts) == 4 and all(x.isdigit() and 0 <= int(x) <= 255 for x in parts))


def sni_of(p):
    """复刻 mihomo 的 SNI 选择：显式 sni/servername > ws Host > server。"""
    hdr = (p.get("ws-opts") or {}).get("headers") or {}
    return p.get("sni") or p.get("servername") or hdr.get("Host") or hdr.get("host")


def tls_on(p):
    if p["type"] == "trojan":
        return True
    return bool(p.get("tls"))


# ---------- 2. 构造去重探测单位 ----------
units = {}          # key -> dict
node_unit = {}      # name -> key
for p in proxies:
    server, port = p["server"], p.get("port", 443)
    if p["type"] == "hysteria2":
        key = ("icmp", server, 0)
        units.setdefault(key, {"kind": "icmp", "target": server, "port": 0,
                               "host": None, "path": None})
    elif not tls_on(p):
        key = ("tcp", server, port)
        units.setdefault(key, {"kind": "tcp", "target": server, "port": port,
                               "host": None, "path": None})
    else:
        sni = sni_of(p)
        hdr = (p.get("ws-opts") or {}).get("headers") or {}
        host = hdr.get("Host") or hdr.get("host") or sni
        path = (p.get("ws-opts") or {}).get("path") or "/"
        if is_domain(server):
            target = server                 # 直连域名：连真实源站
        elif sni and is_domain(sni):
            target = sni                    # CF/CDN IP：用 SNI 域名走 anycast
        else:
            target = server                 # IP+无SNI 的 http 代理
        key = ("tls", target, port, host or "", path)
        units.setdefault(key, {"kind": "tls", "target": target, "port": port,
                               "host": host, "path": path})
    node_unit[p["name"]] = key

print(f"去重后探测单位 {len(units)} 个", flush=True)

# ---------- 3. Globalping 测量 ----------
_ctrl_n = 0


def submit(kind, target, port, country, tag, limit, host=None, path=None):
    if kind == "icmp":
        payload = {"type": "ping", "target": target,
                   "locations": [{"country": country, "tags": [tag]}], "limit": limit}
    elif kind == "tcp":
        payload = {"type": "ping", "target": target,
                   "locations": [{"country": country, "tags": [tag]}], "limit": limit,
                   "measurementOptions": {"protocol": "TCP", "port": port}}
    else:
        headers = {"User-Agent": "Mozilla/5.0"}
        if host:
            headers["Host"] = host
        payload = {"type": "http", "target": target,
                   "locations": [{"country": country, "tags": [tag]}], "limit": limit,
                   "measurementOptions": {"port": port, "protocol": "HTTPS",
                                          "request": {"method": "GET", "path": path or "/",
                                                      "headers": headers}}}
    for a in range(6):
        try:
            resp = requests.post(API, json=payload, headers=H, timeout=20)
        except requests.RequestException:
            time.sleep(10)
            continue
        if resp.status_code == 429:
            time.sleep(30 + 30 * a)
            continue
        if resp.status_code in (200, 202):
            return resp.json()["id"]
        return f"HTTP{resp.status_code}"
    return "RATELIMIT"


def parse(kind, data):
    """每个 probe 一条：(类别, 位置, 详情)。"""
    out = []
    for res in data.get("results", []):
        probe = res.get("probe") or {}
        city = f"{probe.get('city')}/{probe.get('country')}"
        rr = res.get("result") or {}
        if kind in ("icmp", "tcp"):
            st = rr.get("stats") or {}
            if st.get("avg") is not None:
                out.append(("ok", city, "avg=%.0f" % st["avg"]))
            elif st.get("total"):
                out.append(("fail", city, f"loss={st.get('loss')}"))
            else:
                out.append(("err", city, str(rr)[:80]))
        else:
            code = rr.get("statusCode")
            timings = rr.get("timings") or {}
            if timings.get("tls"):
                if code is not None and int(code) >= 500:
                    out.append(("http5xx", city, f"code={code}"))
                else:
                    out.append(("ok", city, f"code={code}"))
            elif code is not None:
                out.append(("tlsfail", city, f"code={code} tls=None"))
            else:
                out.append(("err", city, str(rr.get("error") or rr)[:100]))
    return out


def wait(mid, kind):
    if not mid or not mid.isalnum():
        return None
    for _ in range(45):
        time.sleep(2)
        try:
            d = requests.get(f"{API}/{mid}", headers=H, timeout=20).json()
        except requests.RequestException:
            continue
        if d.get("status") == "finished":
            return parse(kind, d)
    return None


def control():
    """百度对照：家宽必须通，用于发现 probe 拥塞窗口。"""
    global _ctrl_n
    mid = submit("tcp", "www.baidu.com", 443, "CN", "eyeball-network", 2)
    res = wait(mid, "tcp")
    ok = bool(res) and any(x[0] == "ok" for x in res)
    _ctrl_n += 1
    print(f"  [对照#{_ctrl_n} baidu] {'健康' if ok else '异常! ' + str(res)}", flush=True)
    return ok


keys = list(units)


def stage_cn(i_key):
    i, key = i_key
    u = units[key]
    if i and i % 12 == 0:
        if not control():
            time.sleep(45)
            control()
    mid = submit(u["kind"], u["target"], u["port"], "CN", "eyeball-network", 3,
                 u.get("host"), u.get("path"))
    res = wait(mid, u["kind"])
    time.sleep(0.5)
    return key, res


cn_res = {}
done = 0
with ThreadPoolExecutor(max_workers=3) as pool:
    for key, res in pool.map(stage_cn, list(enumerate(keys))):
        cn_res[key] = res
        done += 1
        if done % 10 == 0:
            print(f"  CN 阶段 {done}/{len(keys)}", flush=True)

# ---------- 4. CN 未过单位用 US 复核 ----------
def good_votes(res, kinds=("ok",)):
    if not res:
        return None
    c = Counter(x[0] for x in res)
    return sum(c.get(k, 0) for k in kinds), len(res)


need_us = []
for key in keys:
    g = good_votes(cn_res.get(key))
    if not g or g[0] < 2:
        need_us.append(key)

print(f"CN 表决未过 {len(need_us)} 个，US 复核中…", flush=True)


def stage_us(key):
    u = units[key]
    mid = submit(u["kind"], u["target"], u["port"], "US", None, 2,
                 u.get("host"), u.get("path"))
    return key, wait(mid, u["kind"])


us_res = {}
with ThreadPoolExecutor(max_workers=3) as pool:
    for key, res in pool.map(stage_us, need_us):
        us_res[key] = res
        time.sleep(0.4)

# ---------- 5. 裁决与输出 ----------
def verdict(key):
    u = units[key]
    cn, us = cn_res.get(key), us_res.get(key)
    cn_good = good_votes(cn)
    cn_5xx = good_votes(cn, ("http5xx",))
    us_good = good_votes(us) if us else None
    if cn_good and cn_good[0] >= 2:
        return "CN_OK"
    if cn is None:
        return "CN_UNKNOWN"
    if u["kind"] == "tls" and cn_5xx and cn_5xx[0] >= 2:
        return "ORIGIN_5XX"
    if us_good and us_good[0] >= 1:
        return "CN_BLOCKED"
    if us is None:
        return "BOTH_UNKNOWN"
    return "DEAD_ANYWHERE"


rows = []
vc = Counter()
for p in proxies:
    key = node_unit[p["name"]]
    v = verdict(key)
    vc[v] += 1
    cn, us = cn_res.get(key), us_res.get(key)
    rows.append({
        "name": p["name"], "type": p["type"], "server": p["server"],
        "port": p.get("port"), "mihomo_alive": int(p["name"] in alive),
        "kind": units[key]["kind"], "target": units[key]["target"],
        "host": units[key].get("host") or "", "verdict": v,
        "cn": "|".join(f"{a}@{b}" for a, b, _ in (cn or [])),
        "cn_detail": "|".join(c for _, _, c in (cn or []))[:300],
        "us": "|".join(f"{a}@{b}" for a, b, _ in (us or [])) if us else "",
        "us_detail": "|".join(c for _, _, c in (us or []))[:200] if us else "",
    })

with open("audit3.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

print("\n===== 真SNI TLS/网络层裁决（节点口径）=====", dict(vc), flush=True)

print("\n===== mihomo(境外) × 国内裁决 交叉表 =====")
cross = Counter((int(r["mihomo_alive"]), r["verdict"]) for r in rows)
for (a, v), n in sorted(cross.items(), key=lambda x: (-x[1])):
    print(f"  mihomo={'活' if a else '死'}  {v:14} {n}")

bad = [r for r in rows if r["verdict"] in ("CN_BLOCKED", "ORIGIN_5XX", "DEAD_ANYWHERE")]
dead_abroad = [r for r in rows if not r["mihomo_alive"]]
union = {r["name"] for r in bad} | {r["name"] for r in dead_abroad}
print(f"\n线上 {len(proxies)} 节点中：国内TLS层确认不可用 {len(bad)}；"
      f"境外端到端已死 {len(dead_abroad)}；任一问题 {len(union)}")
print("\n国内层判死样例(前25)：")
for r in bad[:25]:
    print(f"  [{r['verdict']:13}] {r['type']:9} {r['server']}:{r['port']} "
          f"sni={r['host']} mihomo={'活' if r['mihomo_alive'] else '死'}")
