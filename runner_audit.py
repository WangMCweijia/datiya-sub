"""彻查实验2（runner）：mihomo 真实重测 + TLS 层（国内家宽 vs 美国）探测。"""
import logging, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import requests, yaml
import prober

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s", datefmt="%H:%M:%S")
r = requests.get("https://wangmcweijia.github.io/datiya-sub/sub/clash.yaml", timeout=40)
r.encoding = "utf-8"
proxies = yaml.safe_load(r.text)["proxies"]
types0 = Counter(p["type"] for p in proxies)
print(f"线上订阅 {len(proxies)} 节点，类型 {dict(types0)}", flush=True)

cfg = yaml.safe_load(open("config.yaml"))["check"]
alive = prober.filter_working(proxies, cfg)
types1 = Counter(p["type"] for p in alive)
print("\n===== 构建后境外 mihomo 真实重测 =====", flush=True)
print(f"{'类型':11}{'发布时':>7}{'仍存活':>7}{'存活率':>8}")
for t in sorted(types0, key=lambda x: -types0[x]):
    a, b = types0[t], types1.get(t, 0)
    print(f"{t:11}{a:>7}{b:>7}{b/a*100:>7.0f}%")
tot = sum(types0.values())
print(f"{'合计':11}{tot:>7}{len(alive):>7}{len(alive)/tot*100:>7.0f}%")

API = "https://api.globalping.io/v1/measurements"; H = {"User-Agent": "audit/1.0"}
eps = {}
for p in proxies:
    if p.get("port") == 443:
        eps.setdefault(p["server"], set()).add(p["type"])
hosts = list(eps)
print(f"\n对 {len(hosts)} 个 443 主机做 HTTPS 层探测（家宽2点 vs 美国2点）", flush=True)

def submit(host, country, tag):
    loc = {"country": country}
    if tag: loc["tags"] = [tag]
    payload = {"type": "http", "target": host, "limit": 2, "locations": [loc],
               "measurementOptions": {"port": 443, "protocol": "HTTPS",
                  "request": {"method": "HEAD", "path": "/", "headers": {"User-Agent": "Mozilla/5.0"}}}}
    for a in range(4):
        try: resp = requests.post(API, json=payload, headers=H, timeout=20)
        except requests.RequestException: time.sleep(10); continue
        if resp.status_code == 429: time.sleep(20 + 20*a); continue
        if resp.status_code in (200, 202): return resp.json()["id"]
        return f"HTTP{resp.status_code}"
    return "RATELIMIT"

def tls_ok(mid):
    if not mid or not mid.isalnum(): return None
    for _ in range(35):
        time.sleep(2)
        try: dd = requests.get(f"{API}/{mid}", headers=H, timeout=20).json()
        except requests.RequestException: continue
        if dd.get("status") == "finished":
            ok = tot = 0
            for res in dd.get("results", []):
                rr = res.get("result") or {}; tot += 1
                if (rr.get("timings") or {}).get("tls"): ok += 1
            return ok, tot
    return None

def check(host):
    return host, tls_ok(submit(host, "CN", "eyeball-network")), tls_ok(submit(host, "US", None))

results = []
with ThreadPoolExecutor(max_workers=8) as pool:
    for i, row in enumerate(pool.map(check, hosts), 1):
        results.append(row)
        if i % 15 == 0: print(f"  TLS 进度 {i}/{len(hosts)}", flush=True)

cat = Counter(); blocked = []; dead = []
for host, cn, us in results:
    cna = (cn[0] > 0) if cn else None
    usa = (us[0] > 0) if us else None
    cat[(cna, usa)] += 1
    if cna is False and usa: blocked.append(host)
    if cna is False and usa is False: dead.append(host)
print("\n===== TLS 层（国内家宽, 美国）=====")
labels = {(True,True):"两地TLS都通",(False,True):"国内不通境外通(疑似被墙)",
          (False,False):"两地都失败(源站死/非标准TLS)",(True,False):"国内通境外不通",
          (None,True):"国内未知境外通",(None,False):"国内未知境外不通",(None,None):"两地未知"}
for k,n in sorted(cat.items(), key=lambda x:-x[1]):
    print(f"  {labels.get(k,str(k)):34} {n}")
print("疑似被墙:", blocked[:25])
print("源站死/非标准TLS:", dead[:25])
