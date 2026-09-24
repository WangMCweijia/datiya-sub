"""free.datiya.com 免费节点聚合：采集 -> 测活 -> 合并 -> 输出固定订阅。"""

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

import builder
import checker
from fetcher import Fetcher

log = logging.getLogger("datiya")

ROOT = Path(__file__).resolve().parent


def setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def load_config(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def collect(fetcher, days):
    """采集多天节点，返回 (节点列表, 成功的日期列表)。"""
    proxies, ok_days = [], []
    for day in days:
        text = fetcher.fetch_day(day)
        if not text:
            log.warning("跳过 %s：未取到配置", day)
            continue
        parsed = builder.parse_proxies(text)
        log.info("%s：解析到 %d 个节点", day, len(parsed))
        proxies.extend(parsed)
        ok_days.append(day)
    return proxies, ok_days


def render_page(status, out_cfg):
    rows = "".join(
        f"<tr><td>{label}</td><td><code>{path}</code></td></tr>"
        for label, path in (
            ("Clash / Mihomo", out_cfg["clash_file"]),
            ("v2rayN / v2rayNG", out_cfg["v2ray_file"]),
        )
    )
    regions = "、".join(f"{k} {v}" for k, v in status["regions"].items()) or "无"
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>固定订阅 - 免费节点聚合</title>
<style>
  body {{ font-family: system-ui, -apple-system, "Segoe UI", sans-serif; max-width: 720px;
         margin: 40px auto; padding: 0 16px; line-height: 1.7; color: #1a1a1a; }}
  h1 {{ font-size: 1.4rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
  td, th {{ border-bottom: 1px solid #e5e5e5; padding: 8px 4px; text-align: left; }}
  code {{ background: #f4f4f4; padding: 2px 5px; border-radius: 4px; }}
  .muted {{ color: #666; font-size: .9rem; }}
</style>
</head>
<body>
<h1>固定订阅</h1>
<p class="muted">由 <a href="{status['source']}">{status['source']}</a> 每日节点自动聚合生成，
最后更新：<strong>{status['updated_at']}</strong></p>
<table>
  <tr><th>本次采集</th><td>{status['collected']} 个（覆盖 {len(status['days'])} 天）</td></tr>
  <tr><th>可用节点</th><td>{status['alive']} 个</td></tr>
  <tr><th>地区分布</th><td>{regions}</td></tr>
</table>
<table>
  <tr><th>客户端</th><th>订阅文件</th></tr>
  {rows}
</table>
<p class="muted">订阅地址形如 <code>https://&lt;用户名&gt;.github.io/&lt;仓库名&gt;/{out_cfg['clash_file']}</code>，
把它填进 Clash / Mihomo 客户端即可长期使用，无需再更换地址。</p>
</body>
</html>
"""


def write_outputs(out_cfg, clash_text, links, status):
    out_dir = ROOT / out_cfg["dir"]
    clash_path = out_dir / out_cfg["clash_file"]
    v2ray_path = out_dir / out_cfg["v2ray_file"]
    status_path = out_dir / out_cfg["status_file"]

    for path in (clash_path, v2ray_path, status_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    clash_path.write_text(clash_text, encoding="utf-8")
    v2ray_path.write_text(builder.b64("\n".join(links)) + "\n", encoding="utf-8")
    status_path.write_text(
        json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (out_dir / "index.html").write_text(render_page(status, out_cfg), encoding="utf-8")
    log.info("已写入 %s", clash_path.relative_to(ROOT))
    log.info("已写入 %s（%d 条链接）", v2ray_path.relative_to(ROOT), len(links))
    log.info("已写入 %s", status_path.relative_to(ROOT))


def main():
    parser = argparse.ArgumentParser(description="聚合 free.datiya.com 免费节点")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--lookback", type=int, help="覆盖配置中的回溯天数")
    parser.add_argument("--no-check", action="store_true", help="跳过连通性检测")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    cfg = load_config(args.config)

    site, check_cfg, out_cfg = cfg["site"], cfg["check"], cfg["output"]
    lookback = site.get("lookback_days", 3) if args.lookback is None else args.lookback

    fetcher = Fetcher(site)
    days = fetcher.discover_days()[:lookback]
    log.info("本次采集日期：%s", ", ".join(days) or "无")

    proxies, ok_days = collect(fetcher, days)
    if not proxies:
        log.error("未采集到任何节点，保留原有订阅不变")
        return 1

    total = len(proxies)
    proxies = builder.dedupe(proxies)
    log.info("去重后剩余 %d 个节点", len(proxies))

    if check_cfg.get("enabled", True) and not args.no_check:
        proxies = checker.filter_alive(
            proxies,
            timeout=check_cfg.get("timeout", 3.0),
            concurrency=check_cfg.get("concurrency", 200),
        )
    else:
        log.info("已跳过连通性检测")

    if len(proxies) < int(check_cfg.get("min_alive", 1)):
        log.error(
            "可用节点仅 %d 个，低于阈值 %s，保留原有订阅不变",
            len(proxies),
            check_cfg.get("min_alive", 1),
        )
        return 1

    proxies = builder._unique_names(proxies)
    now = datetime.now(timezone(timedelta(hours=8)))
    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S %z"),
        "alive": len(proxies),
        "total": total,
        "days": len(ok_days),
    }
    clash_text = builder.build_clash(proxies, cfg["clash"], meta)
    links = builder.build_links(proxies)

    status = {
        "updated_at": meta["updated_at"],
        "source": site["base_url"],
        "days": ok_days,
        "collected": total,
        "alive": len(proxies),
        "links": len(links),
        "regions": dict(
            sorted(
                Counter(builder.classify(p["name"]) for p in proxies).items(),
                key=lambda kv: (kv[0] == "其他", kv[0]),
            )
        ),
    }
    write_outputs(out_cfg, clash_text, links, status)
    log.info("完成：%d 个可用节点", len(proxies))
    return 0


if __name__ == "__main__":
    sys.exit(main())
