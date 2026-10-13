"""复跑 main.py 的筛选漏斗，并把每个节点按来源标记，统计各源在每一关的留存。"""
import json
import logging
from collections import Counter, OrderedDict

import yaml

import builder
import china
import prober
from fetcher import Fetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("attr")

cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
site, check_cfg = cfg["site"], cfg["check"]
check_cfg["binary"] = ""

fetcher = Fetcher(site)
days = fetcher.discover_days()[: site.get("lookback_days", 7)]
log.info("采集日期：%s", days)

# 顺序与 main.py 一致：先 datiya 站点（7 天），再按 config 顺序的附加源。
# dedupe 是「首次出现保留」，所以重复节点归到更早的来源——与真实流程一致。
tagged, raw = [], Counter()
for day in days:
    text = fetcher.fetch_day(day)
    if not text:
        continue
    for p in builder.parse_proxies(text):
        p["_src"] = "datiya站点"
        tagged.append(p)
        raw["datiya站点"] += 1

for src in cfg.get("sources") or []:
    name = src.get("name")
    text = fetcher.fetch_text(src["url"])
    n = 0
    if text:
        for p in builder.parse_proxies(text):
            p["_src"] = name
            tagged.append(p)
            n += 1
    raw[name] = n
    log.info("附加源 %s：%d 个", name, n)


def dedupe_attr(proxies):
    """与 builder.dedupe 同语义，但判重时忽略 name 和我们加的 _src 标记。"""
    seen, out = set(), []
    for p in proxies:
        key = json.dumps(
            {k: v for k, v in p.items() if k not in ("name", "_src")},
            sort_keys=True, ensure_ascii=False, default=str,
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def tally(proxies):
    c = Counter(p["_src"] for p in proxies)
    return OrderedDict((s, c.get(s, 0)) for s in list(raw))

proxies = tagged
t_raw = tally(proxies)
proxies = dedupe_attr(proxies)
t_dedupe = tally(proxies)
proxies = builder.filter_supported(proxies)
proxies = builder.sanitize(proxies)
proxies = builder._unique_names(proxies)
t_clean = tally(proxies)

name_src = {p["name"]: p["_src"] for p in proxies}
for p in proxies:
    del p["_src"]  # 避免 mihomo 因未知字段拒绝配置

proxies = prober.filter_working(proxies, check_cfg)
c = Counter(name_src[p["name"]] for p in proxies)
t_probe = OrderedDict((s, c.get(s, 0)) for s in list(raw))

china_cfg = check_cfg.get("china_check") or {}
before = len(proxies)
proxies = china.filter_reachable(proxies, china_cfg)
c = Counter(name_src[p["name"]] for p in proxies)
t_cn = OrderedDict((s, c.get(s, 0)) for s in list(raw))

print("\n================ 按订阅源的筛选漏斗（本次实时复跑）================")
print(f"{'来源':<24}{'原始':>6}{'去重后':>7}{'清洗后':>7}{'境外测活':>9}{'国内可达':>9}")
for s in list(raw):
    print(f"{s:<24}{t_raw[s]:>6}{t_dedupe[s]:>7}{t_clean[s]:>7}{t_probe[s]:>9}{t_cn[s]:>9}")
print(f"{'合计':<24}{sum(t_raw.values()):>6}{sum(t_dedupe.values()):>7}{sum(t_clean.values()):>7}{sum(t_probe.values()):>9}{sum(t_cn.values()):>9}")
print(f"\n国内可达性阶段输入 {before}，最终留存 {len(proxies)}")
