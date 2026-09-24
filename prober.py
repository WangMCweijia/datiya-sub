"""用 mihomo（clash.meta）内核做真实测速。

分两轮，都在本地 mihomo 内核里跑：

1. 连通性 + 延迟：逐个请求 test_url（generate_204），拿不到响应的剔除；
   再按 max_delay_ms 卡掉高延迟节点。
2. 真实下载测速：为每个节点单独开一个入站端口（listener 直接绑定该节点），
   经该端口真实下载 speed_url，按「已下载字节 / 耗时」算网速，
   低于 min_speed_kbps 的剔除。

注意：mihomo 自带的 /proxies/{name}/delay 只测首字节时间（TTFB），不下载响应体，
所以网速必须自己走代理下载来测。
"""

import logging
import shutil
import socket
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import requests
import yaml

log = logging.getLogger("datiya.probe")


def _free_port():
    """借系统分配一个空闲端口，避免与其它服务冲突。"""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _find_binary(cfg):
    return shutil.which(cfg.get("binary") or "mihomo")


def _write_config(path, proxies, controller_port, mixed_port, ports):
    """给每个节点配一个绑定它的入站端口，用于第二轮真实下载测速。"""
    listeners = [
        {
            "name": f"probe{i}",
            "type": "mixed",
            "port": ports[proxy["name"]],
            "listen": "127.0.0.1",
            "proxy": proxy["name"],
        }
        for i, proxy in enumerate(proxies)
    ]
    config = {
        "mixed-port": mixed_port,
        "mode": "rule",
        "log-level": "warning",
        "external-controller": f"127.0.0.1:{controller_port}",
        "proxies": proxies,
        "listeners": listeners,
        "rules": ["MATCH,DIRECT"],
    }
    path.write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def _wait_api(proc, base, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            if requests.get(f"{base}/version", timeout=2).status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


def _delay(base, name, test_url, timeout_ms):
    """第一轮：返回 (节点名, 延迟毫秒或 None)。"""
    url = f"{base}/proxies/{quote(name, safe='')}/delay"
    try:
        resp = requests.get(
            url,
            params={"url": test_url, "timeout": timeout_ms},
            timeout=timeout_ms / 1000 + 5,
        )
        if resp.status_code == 200:
            delay = resp.json().get("delay")
            if isinstance(delay, int) and delay > 0:
                return name, delay
    except requests.RequestException:
        pass
    return name, None


def _measure_speed(port, url, budget_bytes, budget_ms):
    """第二轮：经本地端口真实下载，返回 KB/s（失败返回 0）。"""
    proxy = f"http://127.0.0.1:{port}"
    start = time.monotonic()
    received = 0
    try:
        with requests.get(
            url,
            proxies={"http": proxy, "https": proxy},
            stream=True,
            timeout=(5, max(1.0, budget_ms / 1000)),
        ) as resp:
            if resp.status_code != 200:
                return 0.0
            for chunk in resp.iter_content(16384):
                received += len(chunk)
                if received >= budget_bytes:
                    break
                if (time.monotonic() - start) * 1000 >= budget_ms:
                    break
    except requests.RequestException:
        return 0.0
    elapsed = time.monotonic() - start
    if received <= 0 or elapsed <= 0:
        return 0.0
    return received / elapsed / 1024


def _speed_filter(proxies, ports, cfg):
    """第二轮：真实下载测速，剔除下载失败或网速过低的节点。"""
    if not proxies:
        return proxies
    url = cfg.get("speed_url")
    if not url:
        return proxies

    budget_bytes = int(cfg.get("speed_bytes", 1000000))
    budget_ms = int(cfg.get("speed_timeout_ms", 5000))
    min_kbps = float(cfg.get("min_speed_kbps", 0))
    concurrency = int(cfg.get("speed_concurrency", 10))

    log.info("第二轮：真实下载测速 %d 个节点", len(proxies))
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        speeds = list(
            pool.map(
                lambda p: _measure_speed(ports[p["name"]], url, budget_bytes, budget_ms),
                proxies,
            )
        )

    measured = [(p, s) for p, s in zip(proxies, speeds) if s > 0]
    if not measured:
        log.warning("第二轮：所有节点都没测出网速，测速端点可能不可用，跳过网速过滤")
        return proxies
    kept = [p for p, speed in measured if speed >= min_kbps]
    log.info(
        "第二轮：%d/%d 个节点网速 ≥ %g KB/s（成功下载的 %d 个）",
        len(kept),
        len(proxies),
        min_kbps,
        len(measured),
    )
    return kept


def filter_working(proxies, cfg):
    """用 mihomo 内核做两轮真实测速，返回最终可用节点。"""
    exe = _find_binary(cfg)
    if not exe:
        raise FileNotFoundError(
            f"未找到 mihomo 可执行文件（binary={cfg.get('binary') or 'mihomo'}）"
        )

    test_url = cfg.get("test_url", "http://www.gstatic.com/generate_204")
    timeout_ms = int(cfg.get("timeout_ms", 3000))
    concurrency = int(cfg.get("concurrency", 50))
    max_delay_ms = int(cfg.get("max_delay_ms", 0))

    controller_port = _free_port()
    mixed_port = _free_port()
    ports = {p["name"]: _free_port() for p in proxies}
    base = f"http://127.0.0.1:{controller_port}"

    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        config_path = workdir / "config.yaml"
        log_path = workdir / "mihomo.log"
        _write_config(config_path, proxies, controller_port, mixed_port, ports)

        with open(log_path, "wb") as logf:
            proc = subprocess.Popen(
                [exe, "-d", str(workdir), "-f", str(config_path)],
                stdout=subprocess.DEVNULL,
                stderr=logf,
            )
            try:
                if not _wait_api(proc, base):
                    detail = log_path.read_text(encoding="utf-8", errors="replace")
                    raise RuntimeError(f"mihomo 启动失败：{detail.strip()[:500]}")

                names = [p["name"] for p in proxies]
                log.info("第一轮：mihomo 内核就绪，连通性 + 延迟测速 %d 个节点", len(names))
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    delays = dict(
                        pool.map(lambda n: _delay(base, n, test_url, timeout_ms), names)
                    )
                alive = [p for p in proxies if delays.get(p["name"]) is not None]
                if max_delay_ms > 0:
                    alive = [p for p in alive if delays[p["name"]] <= max_delay_ms]
                log.info(
                    "第一轮：%d/%d 个节点连通%s",
                    len(alive),
                    len(names),
                    f"，且延迟 ≤ {max_delay_ms}ms" if max_delay_ms > 0 else "",
                )

                return _speed_filter(alive, ports, cfg)
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
