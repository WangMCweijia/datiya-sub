"""从 free.datiya.com 采集每日 Clash 配置。"""

import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

log = logging.getLogger("datiya.fetch")

POST_LINK_RE = re.compile(r"/post/(\d{8})/")
UPLOAD_RE = re.compile(r"/uploads/(\d{8})-clash\.yaml")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


class Fetcher:
    def __init__(self, site_cfg):
        self.base = site_cfg["base_url"].rstrip("/")
        self.rss = site_cfg.get("rss", "/index.xml")
        self.lookback = int(site_cfg.get("lookback_days", 3))
        self.timeout = float(site_cfg.get("timeout", 20))
        self.retries = max(1, int(site_cfg.get("retries", 3)))
        self.backoff = float(site_cfg.get("retry_backoff", 2.0))
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA, "Accept": "*/*"})

    def _get(self, url):
        """带重试的 GET，返回文本；失败返回 None。"""
        reason = None
        for attempt in range(self.retries):
            try:
                resp = self.sess.get(url, timeout=self.timeout)
                if resp.status_code == 200 and resp.text.strip():
                    return resp.text
                reason = f"HTTP {resp.status_code} / {len(resp.content)} bytes"
            except requests.RequestException as exc:
                reason = str(exc)
            if attempt < self.retries - 1:
                time.sleep(self.backoff ** attempt)
        log.warning("请求失败 %s (%s)", url, reason)
        return None

    def discover_days(self):
        """返回最近若干天的日期列表（YYYYMMDD，从新到旧）。"""
        days = []
        text = self._get(self.base + self.rss)
        if text:
            days = sorted(set(POST_LINK_RE.findall(text)), reverse=True)
        if not days:
            now = datetime.now(timezone(timedelta(hours=8)))
            days = [(now - timedelta(days=i)).strftime("%Y%m%d") for i in range(self.lookback + 4)]
            log.warning("RSS 中未解析到文章，回退为按日期猜测")
        return days[: self.lookback]

    def fetch_day(self, day):
        """获取某天的 clash.yaml 文本，失败返回 None。"""
        text = self._get(f"{self.base}/uploads/{day}-clash.yaml")
        if text and "proxies:" in text:
            return text

        # 文件名规则变化时，从文章页里找真实的订阅链接
        page = self._get(f"{self.base}/post/{day}/")
        if page:
            for match in UPLOAD_RE.finditer(page):
                if match.group(1) != day:
                    continue
                text = self._get(f"{self.base}/uploads/{match.group(1)}-clash.yaml")
                if text and "proxies:" in text:
                    return text
        return None

    def fetch_text(self, url):
        """抓取任意订阅地址的文本，复用同样的超时与重试，失败返回 None。"""
        return self._get(url)