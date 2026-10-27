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

# 稳态测速要丢掉的慢启动窗口（字节）。64KB 太小、1MB 太大：
# 公共代理实测前 200~400KB 的速率只有稳态的一半左右，取 256KB 既跨过慢启动
# 又不会把「只能跑 100KB/s」的节点整个吞掉。
_WARMUP_BYTES = 256 * 1024

# 稳态窗口下限（秒）。低于这个长度的采样对抖动太敏感，宁可多下几百 KB 也要测准。
_MIN_STEADY_S = 1.0

# 稳态窗口的**验收**门槛（比上面的目标小）：窗口里至少要有这么多字节、这么久，
# 这个速率才可信。达不到就退回首字节口径。设这两条是因为一个真实的翻车：
# 慢节点在 8 秒预算快用完时才凑够 256KB 热身，steady_base 刚设好就被超时分支 break，
# 分母变成 0.01 秒 —— 订阅里因此冒出 `316.1MB/s`、`212.6MB/s` 这种不可能的数字。
_VERIFY_WINDOW_BYTES = 256 * 1024
_VERIFY_WINDOW_S = 0.5

# 速率上限（KB/s）。本机到任何免费节点的实测天花板约 12MB/s；超过 100MB/s
# 只可能是计时出了错，宁可判 0（名字显示 --）也不要让假值排到最前面。
_RATE_CAP_KBPS = 100 * 1024


def _alloc_ports(count):
    """分配 count 个互不相同的空闲端口，返回 (端口列表, 持有中的 socket 列表)。

    不能「拿一个就关」：内核会立刻把刚释放的临时端口再次分配出去，
    节点上千时连续申请几乎必然拿到重复端口（还会与先申请后关闭的控制端口撞号），
    导致 mihomo 配置里两个 listener 绑同一端口、启动即失败。
    正确做法：所有 socket 先持有住（保证分配期间不重复），由调用方在启动 mihomo 前统一关闭。
    """
    socks, ports = [], []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            socks.append(sock)
            ports.append(sock.getsockname()[1])
    except BaseException:
        for sock in socks:
            sock.close()
        raise
    return ports, socks


def _find_binary(cfg):
    return shutil.which(cfg.get("binary") or "mihomo")


# mihomo 启动时会确保 geodata（MMDB 等）就绪；拉不到属于网络/环境问题，不是配置本身有误。
_GEO_NOISE = ("MMDB", "GeoIP", "GeoSite", "Geodata", "geoip.metadb", "geosite.dat")


def validate_config(text, cfg):
    """用 `mihomo -t` 校验最终配置能否被内核加载，返回 (是否通过, 说明)。

    节点重名、协议字段非法等都会让内核拒绝**整份**配置，客户端表现为节点列表空白。
    发布前先自检，避免把坏订阅写出去。
    """
    exe = _find_binary(cfg)
    if not exe:
        log.warning("未找到 mihomo（binary=%s），跳过发布前校验", cfg.get("binary") or "mihomo")
        return True, "未找到 mihomo，跳过校验"

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.yaml"
        path.write_text(text, encoding="utf-8")
        try:
            proc = subprocess.run(
                [exe, "-t", "-d", tmp, "-f", str(path)],
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            return False, "mihomo -t 超过 180 秒未结束"

    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part).strip()
    if proc.returncode == 0:
        return True, "通过"

    errors = [line for line in output.splitlines() if "level=error" in line]
    if errors and all(any(key in line for key in _GEO_NOISE) for line in errors):
        log.warning("mihomo 校验因拉取 geodata 失败而报错（非配置问题），放行：%s", errors[0][:200])
        return True, "仅 geodata 相关报错，放行"
    return False, output


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


# 测速端点的候选清单（第一个不够快就换下一个）。判据是「直连能不能跑满」：
# 2026-10-25 本机直连实测 dl.google.com 24 700 KB/s、speed.cloudflare.com 308~446 KB/s。
# 这个差距只影响**直连那一段**（经代理访问时两端都在境外，差距没这么大），所以
# 换端点不是提速的关键，但它决定了预跑与机房侧测量的上限，且明文 HTTP + 大文件 +
# 不跳转能避免 TLS-in-TLS 握手和 302 被算进速率。
DEFAULT_SPEED_URLS = [
    "http://dl.google.com/dl/android/studio/install/2023.1.1.28/"
    "android-studio-2023.1.1.28-windows.exe",
    "https://speed.cloudflare.com/__down?bytes=50000000",
]


def _speed_urls(cfg):
    """从配置里取测速端点候选：优先 speed_urls（列表），兼容旧的 speed_url（单条）。"""
    urls = [u for u in (cfg.get("speed_urls") or []) if u]
    if not urls:
        single = cfg.get("speed_url")
        if single:
            return [single]
        return list(DEFAULT_SPEED_URLS)
    return urls


def _direct_session():
    """不读环境变量的 Session，专供「必须直连」的测量。

    跑在用户自己的机器上时（local_probe），系统里通常装着 Clash/v2ray，
    会导出 `HTTP_PROXY`；CI 沙盒同样会注入。`requests` 默认 `trust_env=True`
    会把这些代理用上，于是「直连预跑端点」实际变成「经用户代理访问端点」：
    测出来的既不是这条线的能力，也不是节点的能力，整条因果链就断了。
    更糟的是代理本身不可用时全部端点都被判失败，这一轮的测速会被静默跳过
    （2026-10-27 实测：沙盒注入的 127.0.0.1:57548 连不上，
    所有端点 `RemoteDisconnected`，speed_url 退回 None）。
    """
    session = requests.Session()
    session.trust_env = False
    return session


def _pick_speed_url(urls, per_url_s=4.0):
    """直连预跑一遍候选端点，挑「小窗口内下得最多」的那个。

    只测 4 秒、不经过任何代理：目的是排除「端点自身就慢」这种系统性误差。
    全部不可达时返回第一个，交给调用方按既定流程失败告警。
    """
    best, best_bytes = None, -1
    for url in urls:
        got = 0
        try:
            with _direct_session().get(
                url,
                stream=True,
                timeout=(4, per_url_s),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status_code != 200:
                    log.warning("测速端点 %s 返回 %s，跳过", url, resp.status_code)
                    continue
                start = time.monotonic()
                for chunk in resp.iter_content(65536):
                    got += len(chunk)
                    if time.monotonic() - start >= per_url_s:
                        break
        except requests.RequestException as exc:
            log.warning("测速端点 %s 直连失败（%s），跳过", url, type(exc).__name__)
            continue
        if got > best_bytes:
            best, best_bytes = url, got
    if best:
        log.info(
            "测速端点选定 %s（直连 4 秒下了 %.1f MB ≈ %.0f KB/s）",
            best,
            best_bytes / 1e6,
            best_bytes / per_url_s / 1024,
        )
        return best
    return urls[0] if urls else None


def _measure_speed(port, url, budget_bytes, budget_ms):
    """第二轮：经本地端口真实下载，返回 KB/s（失败返回 0）。

    只算**稳态速率**，两条都和旧口径不同：

    1. 计时从**首字节到达**开始，不含 TCP+TLS 握手。旧实现把握手算进分母，
       而公共代理的 CONNECT 握手实测 0.8~3.6 秒，6 秒预算里白白扣掉一大截，
       越慢的节点被扣得越狠，快慢被压缩到一起。
    2. 丢掉前 `_WARMUP_BYTES` 的慢启动窗口再计时。TCP 慢启动 + 代理转发缓冲
       让我们看到的前几百 KB 几乎必然偏慢，那一段不代表可持续吞吐。

    代价是「只连得上、下不了多少」的节点会得到 0，这正是我们要区分的那一类。
    """
    proxy = f"http://127.0.0.1:{port}"
    started = time.monotonic()
    received = 0
    first_byte_at = None
    warm_bytes = min(_WARMUP_BYTES, max(1, budget_bytes // 4))
    steady_base = None      # (bytes, mono) 稳态窗口起点
    # trust_env=False：显式 proxies 虽然优先，但环境里的 NO_PROXY 仍可能把
    # 127.0.0.1 排除在代理之外，那样就是「直连测速」，测的是本机带宽不是节点带宽。
    session = _direct_session()
    try:
        with session.get(
            url,
            proxies={"http": proxy, "https": proxy},
            stream=True,
            timeout=(5, max(1.0, budget_ms / 1000)),
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            if resp.status_code != 200:
                return 0.0
            for chunk in resp.iter_content(65536):
                if not chunk:
                    continue
                now = time.monotonic()
                if first_byte_at is None:
                    first_byte_at = now
                received += len(chunk)
                if steady_base is None and received >= warm_bytes:
                    steady_base = (received, now)
                # 字节预算不是为了省流量，是为了别在快节点上白等；但**稳态窗口
                # 不足 1 秒时就提前收工，测出来的数会被慢启动拖低**（合成 5000KB/s
                # 流实测只测出 3573KB/s，-29%）。所以两个条件都满足才停。
                if received >= budget_bytes and (
                    steady_base is None or (now - steady_base[1]) >= _MIN_STEADY_S
                ):
                    break
                if (now - started) * 1000 >= budget_ms:
                    break
    except requests.RequestException:
        return 0.0
    if received <= 0 or first_byte_at is None:
        return 0.0
    end = time.monotonic()
    steady_ok = (
        steady_base is not None
        and (received - steady_base[0]) >= _VERIFY_WINDOW_BYTES
        and (end - steady_base[1]) >= _VERIFY_WINDOW_S
    )
    if steady_ok:
        base_bytes, base_at = steady_base
    else:
        # 稳态窗口没验收通过（热身刚结束就超时 / 下得太少）：退回首字节口径。
        # 分母至少是整段下载时间，不会被 0.01 秒放大成几百 MB/s。
        base_bytes, base_at = 0, first_byte_at
    elapsed = end - base_at
    if elapsed <= 0:
        return 0.0
    kbps = (received - base_bytes) / elapsed / 1024
    if kbps > _RATE_CAP_KBPS:
        # 退回全段平均再判一次：仍然离谱就说明这次采样本身无效。
        kbps = received / max(1e-6, end - first_byte_at) / 1024
        if kbps > _RATE_CAP_KBPS:
            return 0.0
    return kbps


def _speed_filter(proxies, ports, cfg):
    """第二轮：真实下载测速，剔除下载失败或网速过低的节点。"""
    if not proxies:
        return proxies
    url = _pick_speed_url(_speed_urls(cfg))
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
    # 把实测网速挂到节点上（输出前由 main 剥除），并按网速从高到低排序。
    # sorted 是稳定排序：网速相同的节点保持进入本轮前的顺序（那时已按延迟从低到高排好）。
    for proxy, speed in measured:
        proxy["_speed_kbps"] = round(speed, 1)
    kept = [p for p, speed in measured if speed >= min_kbps]
    kept.sort(key=lambda p: -p["_speed_kbps"])
    log.info(
        "第二轮：%d/%d 个节点网速 ≥ %g KB/s（成功下载的 %d 个），已按网速从高到低排序",
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

    # 节点上千时要同时持有数千个占位 socket、mihomo 还要监听数千个端口；
    # 部分机器进程的文件描述符软上限只有 1024，会直接耗尽。尽量抬到硬上限（子进程继承）。
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = hard if hard != resource.RLIM_INFINITY else 65536
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except (ValueError, OSError, ImportError):
        pass

    # 控制端口、混合入站端口和每个节点的测速端口在同一批次里占位，
    # 保证几千个端口彼此不撞号（分开申请会让内核把刚释放的端口再发出来）。
    all_ports, held_sockets = _alloc_ports(len(proxies) + 2)
    controller_port, mixed_port = all_ports[0], all_ports[1]
    ports = {p["name"]: port for p, port in zip(proxies, all_ports[2:])}
    base = f"http://127.0.0.1:{controller_port}"

    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        config_path = workdir / "config.yaml"
        log_path = workdir / "mihomo.log"
        # 配置写好后立即释放占位 socket，紧接着启动 mihomo 绑定这些端口。
        try:
            _write_config(config_path, proxies, controller_port, mixed_port, ports)
        finally:
            for sock in held_sockets:
                sock.close()

        with open(log_path, "wb") as logf:
            proc = subprocess.Popen(
                [exe, "-d", str(workdir), "-f", str(config_path)],
                stdout=logf,
                stderr=subprocess.STDOUT,
            )
            try:
                if not _wait_api(proc, base, timeout=60.0):
                    detail = log_path.read_text(encoding="utf-8", errors="replace")
                    raise RuntimeError(f"mihomo 启动失败：{detail.strip()[:500]}")

                names = [p["name"] for p in proxies]
                log.info("第一轮：mihomo 内核就绪，连通性 + 延迟测速 %d 个节点", len(names))
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    delays = dict(
                        pool.map(lambda n: _delay(base, n, test_url, timeout_ms), names)
                    )
                # 把延迟挂到节点上：发布顺序不按它排（只按网速，见 builder.sort_by_speed），
                # 但 `check.max_delay_ms` 的存活闸门要用。`_` 前缀字段在输出前会被
                # main 剥掉，不会写进订阅与候选池。
                alive = []
                for p in proxies:
                    d = delays.get(p["name"])
                    if d is None:
                        continue
                    p["_delay_ms"] = int(d)
                    alive.append(p)
                if max_delay_ms > 0:
                    alive = [p for p in alive if p["_delay_ms"] <= max_delay_ms]
                # 按延迟从低到高排序，让「更值得保留」的快节点排在前面。
                # 第二轮会按网速重排；此处顺序也是第三轮国内可达性的探测顺序：
                # Globalping 免费额度有限，配额耗尽后剩下的只能记为未知、在 strict
                # 模式下被剔除——先探快节点，被名额挤掉的才是慢节点。
                alive.sort(key=lambda p: delays[p["name"]])
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
