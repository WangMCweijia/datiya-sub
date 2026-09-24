"""用 mihomo（clash.meta）内核做真实代理测速。

把候选节点交给本地 mihomo 内核，逐个请求 test_url 测真实延迟：
拿不到延迟的节点（协议/凭据已失效、无法出网）会被直接剔除，
比只测 TCP 端口能否连上准确得多。
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
    exe = cfg.get("binary") or "mihomo"
    return shutil.which(exe)


def _write_config(path, proxies, controller_port, mixed_port):
    config = {
        "mixed-port": mixed_port,
        "mode": "rule",
        "log-level": "warning",
        "external-controller": f"127.0.0.1:{controller_port}",
        "proxies": proxies,
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
    """返回 (节点名, 延迟毫秒或 None)。"""
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


def filter_working(proxies, cfg):
    """用 mihomo 内核真实测速，返回能成功访问 test_url 的节点。"""
    exe = _find_binary(cfg)
    if not exe:
        raise FileNotFoundError(
            f"未找到 mihomo 可执行文件（binary={cfg.get('binary') or 'mihomo'}）"
        )

    test_url = cfg.get("test_url", "http://www.gstatic.com/generate_204")
    timeout_ms = int(cfg.get("timeout_ms", 3000))
    concurrency = int(cfg.get("concurrency", 50))

    controller_port = _free_port()
    mixed_port = _free_port()
    base = f"http://127.0.0.1:{controller_port}"

    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        config_path = workdir / "config.yaml"
        log_path = workdir / "mihomo.log"
        _write_config(config_path, proxies, controller_port, mixed_port)

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
                log.info("mihomo 内核就绪，开始真实测速：%d 个节点", len(names))
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    results = list(
                        pool.map(lambda n: _delay(base, n, test_url, timeout_ms), names)
                    )
                working = {name for name, delay in results if delay is not None}
                alive = [p for p in proxies if p["name"] in working]
                log.info("真实测速：%d/%d 个节点可用", len(alive), len(names))
                return alive
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
