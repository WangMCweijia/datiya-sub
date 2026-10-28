"""本机（国内家宽）真实代理测速：产出 whitelist.json 供 CI 过滤。

为什么需要它
------------
CI 的两道筛选都不在国内：mihomo 测速跑在 GitHub Actions（美国机房），
Globalping 只发一次 TLS 握手。GFW 常「放握手、掐流量」，于是「境外能连」
与「国内能用」几乎是两件事——实测通过全部筛选的 169 个节点只有 78 个真能用。

这个脚本在你自己的国内机器上，用 mihomo 内核给每个候选节点开一个独立入站端口，
逐个真实 CONNECT `generate_204` 并走代理下载测速。判定结果与客户端实际体验一致
（同一方法测出的 http 可用率 84%、vless 10%，与手工体验吻合）。

用法
----
    python local_probe.py                    # 测一轮，更新本地 whitelist.json
    python local_probe.py --push             # 测完推送到 GitHub（需 gh 已登录）
    python local_probe.py --push --loop 1800 # 常驻：每 30 分钟测一轮并推送
    python local_probe.py --max 50 -v        # 调试：只测 50 个候选

依赖：pip install pyyaml requests
内核：--binary 指定，或环境变量 MIHOMO_BIN，或 ./tools/mihomo.exe、./mihomo.exe、
      PATH 中的 mihomo。run_local_probe.bat 会在缺失时自动下载。
"""

import argparse
import base64
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

import builder
import prober
import whitelist

ROOT = Path(__file__).resolve().parent
TZ = timezone(timedelta(hours=8))
log = logging.getLogger("datiya.local")

_LOG_MAX_BYTES = 20 * 1024 * 1024

# 计划任务用 pythonw.exe 跑（没有控制台），子进程默认会各自弹一个黑窗。
# CREATE_NO_WINDOW 让 gh 和 mihomo 都不闪窗；非 Windows 上这个常量为 0。
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _rotate_log(path):
    """日志超过 20MB 就滚成 .1。计划任务每 3 小时跑一轮，一年能涨到几十 MB。"""
    try:
        if path.exists() and path.stat().st_size > _LOG_MAX_BYTES:
            backup = path.with_name(path.name + ".1")
            if backup.exists():
                backup.unlink()
            path.rename(backup)
    except OSError:
        pass  # 日志滚不动不值得让整轮测速失败


def now_stamp():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %z")


def parse_stamp(text):
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(str(text), fmt)
        except ValueError:
            continue
    return None


# --- 候选池获取 -------------------------------------------------------------


def detect_repo():
    """从 git remote 猜仓库名；失败则用默认值。"""
    try:
        url = subprocess.check_output(
            ["git", "remote", "get-url", "origin"], cwd=ROOT, text=True, timeout=10
        ).strip()
    except Exception:
        return "WangMCweijia/datiya-sub"
    if url.startswith("git@github.com:"):
        url = url.split(":", 1)[1]
    if "github.com/" in url:
        slug = url.split("github.com/", 1)[1]
        return slug[:-4] if slug.endswith(".git") else slug
    return "WangMCweijia/datiya-sub"


def _gh(args, stdin=None, direct=False):
    """调 gh CLI。direct=True 时剥掉代理环境变量（本机代理偶发 TLS handshake timeout，
    直连与代理交替重试才能穿过这种抖动）。"""
    env = dict(os.environ)
    if direct:
        for key in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy",
                    "ALL_PROXY", "all_proxy"):
            env.pop(key, None)
    return subprocess.run(
        ["gh"] + args, input=stdin, text=True, capture_output=True,
        encoding="utf-8", env=env, creationflags=_NO_WINDOW,
    )


def fetch_candidates(repo, path=None, name="candidates.json.gz"):
    """优先从仓库拿 CI 最新一轮的候选池（比本地副本新）。"""
    if path:
        items = whitelist.load_candidates(Path(path))
        log.info("候选池来自本地文件 %s（%d 个）", path, len(items))
        return items

    sources = [
        ("raw.githubusercontent",
         f"https://raw.githubusercontent.com/{repo}/main/{name}"),
        ("ghfast 镜像",
         f"https://ghfast.top/https://raw.githubusercontent.com/{repo}/main/{name}"),
    ]
    for label, url in sources:
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                items = whitelist.load_candidates_bytes(resp.content)
                if items:
                    log.info("候选池来自 %s（%d 个）", label, len(items))
                    return items
            log.warning("%s 返回 %s，换下一个源", label, resp.status_code)
        except Exception as exc:
            log.warning("%s 拉取失败：%s", label, exc)

    for direct in (True, False):
        proc = _gh(["api", f"repos/{repo}/contents/{name}", "--jq", ".content"],
                   direct=direct)
        if proc.returncode == 0 and proc.stdout.strip():
            items = whitelist.load_candidates_bytes(
                base64.b64decode(proc.stdout.strip())
            )
            if items:
                log.info("候选池来自 gh api（%d 个）", len(items))
                return items
    raise SystemExit(
        "拿不到 %s：请确认仓库里已有一轮 CI 构建，或用 --candidates 指定本地文件" % name
    )


# --- 内核 -------------------------------------------------------------------


def resolve_binary(explicit):
    candidates = []
    if explicit:
        candidates.append(explicit)
    if os.environ.get("MIHOMO_BIN"):
        candidates.append(os.environ["MIHOMO_BIN"])
    candidates += [
        str(ROOT / "tools" / "mihomo.exe"),
        str(ROOT / "mihomo.exe"),
        str(ROOT / "mhcore" / "mihomo-windows-amd64.exe"),
    ]
    for cand in candidates:
        if cand and Path(cand).exists():
            return str(cand)
    found = shutil.which("mihomo")
    if found:
        return found
    raise SystemExit(
        "未找到 mihomo 内核。请用 --binary 指定，或运行 run_local_probe.bat（会自动下载）。"
    )


# --- 探测 -------------------------------------------------------------------


def probe(candidates, exe, args, check_cfg):
    """给每个候选开独立入站端口，真实 CONNECT + 下载测速，返回 [(候选, 结果)]。"""
    proxies = []
    for index, item in enumerate(candidates):
        proxy = {k: v for k, v in item.items() if not k.startswith("_")}
        proxy["name"] = "p%04d" % index
        proxies.append(proxy)
    proxies = builder.filter_supported(proxies)
    if not proxies:
        raise SystemExit("候选池里没有内核能加载的节点")
    log.info("候选 %d 个，内核可加载 %d 个", len(candidates), len(proxies))

    test_url = args.test_url or check_cfg.get("test_url") or "https://www.gstatic.com/generate_204"
    timeout_ms = int(args.timeout_ms or check_cfg.get("timeout_ms") or 3000)
    concurrency = int(args.concurrency or check_cfg.get("concurrency") or 50)
    # 端点先按「本机直连能不能跑满」预跑一遍再选定：白名单的全部意义就是
    # 「从国内这条线能跑多快」，端点自己只有 300KB/s 的话，测出来全是一片慢。
    speed_url = args.speed_url or prober._pick_speed_url(prober._speed_urls(check_cfg))
    speed_bytes = int(check_cfg.get("speed_bytes") or 1000000)
    speed_ms = int(check_cfg.get("speed_timeout_ms") or 5000)
    speed_conc = int(check_cfg.get("speed_concurrency") or 12)

    all_ports, held = prober._alloc_ports(len(proxies) + 2)
    controller_port, mixed_port = all_ports[0], all_ports[1]
    ports = {p["name"]: port for p, port in zip(proxies, all_ports[2:])}
    base = f"http://127.0.0.1:{controller_port}"

    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        config_path = workdir / "config.yaml"
        log_path = workdir / "mihomo.log"
        try:
            prober._write_config(config_path, proxies, controller_port, mixed_port, ports)
        finally:
            for sock in held:
                sock.close()

        with open(log_path, "wb") as logf:
            proc = subprocess.Popen(
                [exe, "-d", str(workdir), "-f", str(config_path)],
                stdout=logf,
                stderr=subprocess.STDOUT,
                creationflags=_NO_WINDOW,
            )
            try:
                if not prober._wait_api(proc, base, timeout=90.0):
                    detail = log_path.read_text(encoding="utf-8", errors="replace")
                    raise SystemExit("mihomo 启动失败：\n" + detail.strip()[:800])

                # 分批扫：一次性把上千个节点同时拨出去，会把自己拨到「假死」。
                # 实测（2026-10-22 本机）：242 个订阅节点一次并发 30 测出 107 个活（44%），
                # 把判死的 135 个单独装一个小实例、并发 4 复测，又有 42 个活过来
                # （合并 149 个 = 62%）——那 42 个是被上一轮的连接风暴压死的，不是死了。
                # 免费公共代理普遍对「源 IP 的瞬时并发/频次」限流，所以既要分批，
                # 也要对失败节点低并发复测一次；否则白名单会长期低估候选池。
                chunk = int(check_cfg.get("chunk") or 0)
                gap = float(check_cfg.get("chunk_gap") or 0)
                retry_conc = int(check_cfg.get("retry_concurrency") or 4)

                def sweep(items, conc, label):
                    """按 chunk 分批跑 n 轮延迟测试，返回 {name: ms or None}。"""
                    out = {}
                    step = chunk if chunk > 0 else len(items) or 1
                    for start in range(0, len(items), step):
                        part = items[start:start + step]
                        with ThreadPoolExecutor(max(1, conc)) as pool:
                            out.update(
                                dict(
                                    pool.map(
                                        lambda p: prober._delay(
                                            base, p["name"], test_url, timeout_ms
                                        ),
                                        part,
                                    )
                                )
                            )
                        if gap and start + step < len(items):
                            time.sleep(gap)
                    return out

                log.info("第一轮：真实 CONNECT %d 个节点（并发 %d，每批 %s）",
                         len(proxies), concurrency, chunk or len(proxies))
                delays = sweep(proxies, concurrency, "第一轮")
                fails = [p for p in proxies if not delays.get(p["name"])]
                if fails and retry_conc:
                    log.info("第一轮补测：%d 个未通过，低并发复测（并发 %d，每批 %s）",
                             len(fails), retry_conc, chunk or len(fails))
                    again = sweep(fails, retry_conc, "补测")
                    rescued = 0
                    for p in fails:
                        if again.get(p["name"]):
                            delays[p["name"]] = again[p["name"]]
                            rescued += 1
                    log.info("第一轮补测：救回 %d 个（此前被判「不可用」）", rescued)

                alive = [(p, delays[p["name"]]) for p in proxies if delays.get(p["name"])]
                log.info("第一轮：可真实出网 %d/%d 个节点", len(alive), len(proxies))

                speeds = {}
                if speed_url and alive:
                    log.info("第二轮：走代理下载测速 %d 个节点（并发 %d）", len(alive), speed_conc)
                    with ThreadPoolExecutor(speed_conc) as pool:
                        measured = list(
                            pool.map(
                                lambda item: prober._measure_speed(
                                    ports[item[0]["name"]], speed_url, speed_bytes, speed_ms
                                ),
                                alive,
                            )
                        )
                    speeds = {item[0]["name"]: kb for item, kb in zip(alive, measured)}
                    got = sum(1 for kb in speeds.values() if kb > 0)
                    log.info("第二轮：%d/%d 个测出网速", got, len(alive))
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

    results = {}
    for proxy in proxies:
        fp = builder.fingerprint(proxy)
        delay = dict(delays).get(proxy["name"])
        results[fp] = {
            "ok": bool(delay),
            "delay": int(delay or 0),
            "kbps": round(float(speeds.get(proxy["name"]) or 0), 1),
            "type": str(proxy.get("type")),
            "line": "%s:%s" % (proxy.get("server"), proxy.get("port")),
        }
    return results


class ThreadPoolExecutorSafe:
    """ThreadPoolExecutor 的轻量包装：保证 map 结果顺序与输入一致（同标准实现）。"""

    def __init__(self, workers):
        
        self._pool = ThreadPoolExecutor(max_workers=max(1, int(workers)))

    def __enter__(self):
        return self._pool

    def __exit__(self, *exc):
        self._pool.shutdown(wait=True)
        return False


# --- 合并与落盘 -------------------------------------------------------------


def _backfill_first_ok(stamp, rounds, interval_h):
    """老条目没有 first_ok 时倒推一个起点。

    只有 1 轮测通时起点就是本轮（真实时间，不标推算）；多轮时按
    「(已测通轮数 - 1) × 轮间隔」往前推。推算出来的起点会被标 first_ok_est，
    订阅名字里带 `~` 提醒读者这是估的。
    """
    if rounds <= 1 or interval_h <= 0:
        return stamp
    when = parse_stamp(stamp)
    if not when:
        return stamp
    back = when - timedelta(hours=(rounds - 1) * interval_h)
    return back.strftime("%Y-%m-%d %H:%M:%S %z")


def merge(old, results, keep_days, dead_after, round_no=0, interval_h=3.0):
    """把本轮结果并入历史：ok_rounds 累加，连续 3 轮失败标记 dead。

    保留历史很重要：免费节点会抖动，单轮失败不代表死，下一轮恢复即可；
    反之一次侥幸通过也不代表稳，ok_rounds 让 CI 能优先选真正长寿的。

    `first_ok` 是「这一段连续存活」的起点：订阅名字里的存活时长、以及
    「老黄牛优先」排序都读它。只在测通时写，被判死（dead）后才重置。
    """
    entries = dict(old.get("entries") or {})
    stamp = now_stamp()
    for fp, res in results.items():
        entry = dict(entries.get(fp) or {})
        # 被判死之后又活过来：上一段连续存活已经结束，起点必须重置，
        # 否则中断的那几天会被算进存活时长（名字里就成了假的「活了 5 天」）。
        was_dead = bool(entry.get("dead"))
        # 回填起点要用**本轮之前**的轮数与上次测通时间：entry 马上就要被本轮结果覆盖，
        # 拿覆盖后的值倒推会少算一轮（first_ok 每轮往前漂一个间隔，越漂越早）。
        prev_rounds = int(entry.get("ok_rounds") or 0)
        prev_last_ok = entry.get("last_ok")
        # round 记录「最后一次被实测是第几轮」：CI 据此只发**本轮**测通的节点。
        # 只靠 last_ok 时间窗不行——一轮 1200 个节点要测好几分钟，窗口稍宽就会把
        # 上一轮的历史命中放进来（实测：发布 390 个，其中 208 个是上一轮的旧结果，
        # 复测只剩 59% 可用；而本轮新测的 250 个里 229 个仍可用，存活率 92%）。
        # last_round = 最后被「测过」的轮次（通过与否都更新）；round 只在**测通**时更新。
        # 两者必须分开：早先的实现把 round 在失败分支也刷新，于是本轮挂掉的节点
        # 带着「round=本轮」溜过 CI 的轮次校验（实测第 4 轮 470 个条目标记成本轮，
        # 实际测通的只有 245 个），白名单等于没过滤。
        entry.update({"line": res["line"], "type": res["type"], "last_check": stamp,
                      "last_round": int(round_no)})
        if res["ok"]:
            entry["round"] = int(round_no)
            entry["ok_rounds"] = int(entry.get("ok_rounds") or 0) + 1
            entry["fail_rounds"] = 0
            entry["delay"] = res["delay"]
            entry["kbps"] = res["kbps"]
            entry["last_ok"] = stamp
            entry.pop("dead", None)
            if was_dead:
                entry.pop("first_ok", None)
                entry.pop("first_ok_est", None)
            if not entry.get("first_ok"):
                if was_dead or prev_rounds <= 0 or not prev_last_ok:
                    # 全新的连续段：本轮就是真实起点。
                    entry["first_ok"] = stamp
                else:
                    # 升级前的老条目：用「上次测通时间 - (已测通轮数-1) × 间隔」倒推。
                    # 只测通过一次时倒推结果就是上次测通时间，属于真实值，不标推算。
                    entry["first_ok"] = _backfill_first_ok(
                        prev_last_ok, prev_rounds, float(interval_h or 0)
                    )
                    if prev_rounds > 1:
                        entry["first_ok_est"] = True
        else:
            entry["fail_rounds"] = int(entry.get("fail_rounds") or 0) + 1
            if entry["fail_rounds"] >= dead_after:
                entry["dead"] = True
        entries[fp] = entry

    cutoff = datetime.now(TZ) - timedelta(days=keep_days)
    pruned = 0
    for fp in list(entries):
        entry = entries[fp]
        if entry.get("dead") or not int(entry.get("ok_rounds") or 0):
            entries.pop(fp)
            pruned += 1
            continue
        last_ok = parse_stamp(entry.get("last_ok"))
        if last_ok and last_ok < cutoff:
            entries.pop(fp)
            pruned += 1
    return entries, pruned


def build_payload(entries, host, rounds):
    ordered = dict(
        sorted(
            entries.items(),
            key=lambda kv: (
                -int(kv[1].get("ok_rounds") or 0),
                -float(kv[1].get("kbps") or 0),
                int(kv[1].get("delay") or 99999),
            ),
        )
    )
    return {
        "version": whitelist.VERSION,
        "generated_at": now_stamp(),
        "probe_host": host,
        "probe_rounds": rounds,
        "note": "本机（国内家宽）mihomo 真实代理测速结果，仅本文件中的节点会被 CI 发布",
        "entries": ordered,
    }


_PUSH_ATTEMPTS = 12


def _is_sha_conflict(err):
    """判断是不是「sha 过期」这种需要重取 sha 的错误。

    别写成 `"sha" in err.lower()`：**"handshake" 里就含 "sha"**（handSHAke），
    于是每次 `TLS handshake timeout` 都被当成 sha 冲突 → 直接 break 掉内层循环，
    代理通道那一半尝试全被跳过。实测下来等于把 8 轮重试砍成 8 次单通道尝试。
    """
    low = err.lower()
    return "does not match" in low or "sha wasn't supplied" in low


def _push_backoff(attempt):
    """退避 2,5,8,11,14,17,18,18…（秒）。总计约 165 秒，覆盖测速刚结束那段握手抖动。"""
    return min(2 + attempt * 3, 18)


def push(payload, repo, path):
    content = base64.b64encode(
        json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
    ).decode()
    # 到 api.github.com 的 TLS 握手在本机是**间歇性**失败的：直连和代理各有自己的
    # 抖动窗口，所以每次「取 sha + PUT」都直连、代理各试一遍，再重复多轮。
    # 早先 6 次各只试一种通道，实测 6 次全灭（3 次握手超时 + 3 次拿不到 sha）。
    last_err = ""
    for attempt in range(_PUSH_ATTEMPTS):
        sha = ""
        for direct in (True, False):
            proc = _gh(["api", f"repos/{repo}/contents/{path}", "--jq", ".sha"],
                       direct=direct)
            if proc.returncode == 0 and proc.stdout.strip():
                sha = proc.stdout.strip()
                break
        if not sha:
            # 取不到 sha 时绝不能直接 PUT：文件已存在而无 sha 会被 422 拒掉，
            # 白等一轮重试还不知道原因（踩过：6 次全 422 "sha wasn't supplied"）。
            log.warning("第 %d 轮：取不到 %s 的 sha，稍后重试", attempt + 1, path)
            time.sleep(_push_backoff(attempt))
            continue
        body = {
            "message": "chore: 更新本机实测白名单 %s（%d 个可用）"
            % (datetime.now(TZ).strftime("%m-%d %H:%M"), len(payload["entries"])),
            "content": content,
            "branch": "main",
            "sha": sha,
        }
        for direct in (True, False):
            proc = _gh(
                ["api", "--method", "PUT", f"repos/{repo}/contents/{path}", "--input", "-"],
                stdin=json.dumps(body),
                direct=direct,
            )
            if proc.returncode == 0:
                log.info("已推送 %s 到 %s（第 %d 轮）", path, repo, attempt + 1)
                return True
            last_err = (proc.stderr or proc.stdout or "").strip()
            if _is_sha_conflict(last_err):
                break  # sha 过期（CI 同时提交）→ 重取 sha，换下一轮
        log.warning("推送失败（第 %d 轮）：%s", attempt + 1,
                    last_err.splitlines()[-1][:160] if last_err else "未知错误")
        time.sleep(_push_backoff(attempt))
    log.error("推送失败，白名单只留在本地（CI 会因白名单过期而暂停更新，保留上一版订阅）")
    return False


def report(payload, results, rounds):
    entries = payload["entries"]
    ok_now = sum(1 for res in results.values() if res["ok"])
    total = len(results)
    print()
    print("=" * 62)
    print(f"第 {rounds} 轮本机实测：{ok_now}/{total} 个候选可真实代理出网 "
          f"（{ok_now / max(1, total) * 100:.0f}%）")
    by_type = {}
    for fp, res in results.items():
        stat = by_type.setdefault(res["type"], [0, 0])
        stat[1] += 1
        if res["ok"]:
            stat[0] += 1
    print("按协议：")
    for kind, (ok, count) in sorted(by_type.items(), key=lambda kv: -kv[1][1]):
        print(f"  {kind:10s} {ok:3d}/{count:3d}  {ok / count * 100:5.1f}%")
    # 只看本轮、并且**必须排序**：entries 是累计表（含历史轮次），
    # 直接取前 8 个打印出来的是字典插入顺序，第一行可能是某个 47KB/s 的老条目，
    # 让人误判「这一轮没测出快的」。
    fast = sorted(
        (
            entry
            for entry in entries.values()
            if float(entry.get("kbps") or 0) > 0
            and int(entry.get("round") or 0) == int(rounds)
        ),
        key=lambda entry: -float(entry.get("kbps") or 0),
    )[:8]
    if fast:
        print("本轮最快的几个（KB/s）：")
        for entry in fast:
            print(f"  {entry['kbps']:8.0f}  {entry['delay']:5d}ms  "
                  f"{entry['type']:8s} {entry['line']}")
    print(f"白名单总条目：{len(entries)}（含历史；CI 只发其中的节点）")
    print("=" * 62)


def main():
    parser = argparse.ArgumentParser(description="本机真实代理测速，产出白名单")
    parser.add_argument("--candidates", help="直接用本地 candidates.json，不联网拉取")
    parser.add_argument("--repo", help="GitHub 仓库 owner/name（默认从 git remote 推断）")
    parser.add_argument("--binary", help="mihomo 可执行文件路径")
    parser.add_argument("--max", type=int, default=0, help="最多测几个候选（调试用）")
    parser.add_argument("--host", default="", help="探测点标识，写进 whitelist.json")
    parser.add_argument("--push", action="store_true", help="测完推送到 GitHub")
    parser.add_argument("--loop", type=int, default=0, help="常驻：每 N 秒跑一轮（0=只跑一次）")
    parser.add_argument("--keep-days", type=int, default=7, help="历史条目保留天数")
    parser.add_argument("--dead-after", type=int, default=3, help="连续几轮失败判死")
    parser.add_argument("--test-url", help="连通性测试地址（覆盖配置）")
    parser.add_argument("--speed-url", help="下载测速地址（覆盖配置）")
    parser.add_argument("--timeout-ms", type=int, help="单节点连通测试超时（毫秒）")
    parser.add_argument("--concurrency", type=int, help="连通测试并发")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--log-file", help="同时把日志写进文件（计划任务里 stdout 重定向不可靠）")
    args = parser.parse_args()

    log_path = Path(args.log_file) if args.log_file else None
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        _rotate_log(log_path)

    # pythonw.exe（计划任务用）没有控制台，sys.stdout / sys.stderr 是 None，
    # report() 里的 print() 会直接抛 AttributeError。把它们接到日志文件上：
    # 既让 print 安全，又让那份人读的测速摘要留在同一个文件里。
    console_less = sys.stdout is None or sys.stderr is None
    sink = None
    if console_less:
        sink = open(log_path or os.devnull, "a", encoding="utf-8", buffering=1)
        if sys.stdout is None:
            sys.stdout = sink
        if sys.stderr is None:
            sys.stderr = sink

    handlers = [logging.StreamHandler()]
    if log_path and not console_less:
        # FileHandler 只在有控制台时才加：pythonw 下 stderr 已经指向同一个文件，
        # 再加一个 handler 会让每行日志重复两遍。
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-6s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )

    repo = args.repo or detect_repo()
    cfg_path = ROOT / "config.yaml"
    check_cfg = {}
    if cfg_path.exists():
        check_cfg = (yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}).get("check") or {}
    wl_cfg = check_cfg.get("whitelist") or {}
    wl_path = ROOT / str(wl_cfg.get("file", "whitelist.json"))
    exe = resolve_binary(args.binary)
    host = args.host or os.environ.get("COMPUTERNAME") or "local"

    rounds = 0
    while True:
        rounds += 1
        candidates = fetch_candidates(
            repo, args.candidates,
            (check_cfg.get("candidates") or {}).get("file", "candidates.json.gz"),
        )
        if args.max:
            candidates = candidates[: args.max]
        try:
            results = probe(candidates, exe, args, check_cfg)
        except SystemExit:
            raise
        except Exception as exc:
            log.error("本轮测速异常：%s", exc)
            if not args.loop:
                raise
            time.sleep(args.loop)
            continue

        old = whitelist.load(wl_path)
        round_no = int(old.get("probe_rounds") or 0) + 1
        # 轮间隔只用来给「升级前的老条目」倒推存活起点，取计划任务的周期（默认 3 小时）。
        interval_h = float(
            (check_cfg.get("whitelist") or {}).get("round_interval_hours", 3) or 3
        )
        entries, pruned = merge(
            old, results, args.keep_days, args.dead_after, round_no, interval_h
        )
        payload = build_payload(entries, host, round_no)
        # newline="\n" 不能省：Windows 上文本模式会把每个 \n 翻成 \r\n，而 CI（Linux）
        # 和 GitHub 上的 blob 都是 LF。不统一的话本地 182KB 的文件在仓库里被当成
        # 「整文件重写」，既污染 diff 又让 push_rest.py 的哈希比对全部报警。
        wl_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        report(payload, results, payload["probe_rounds"])
        log.info("已写入 %s（历史清理 %d 条）", wl_path.name, pruned)

        if args.push:
            # 刚测完的上千条连接会让本机对 api.github.com 的新 TLS 握手持续失败
            # （实测：测完立刻推，8 轮 16 次全灭，全是 handshake timeout；
            #  停一分钟后再推，第 3 次就成功）。冷却一下再推，省掉无谓重试。
            cooldown = float(os.environ.get("PUSH_COOLDOWN", "45") or 0)
            if cooldown:
                log.info("推送前冷却 %.0f 秒（刚测完，链路还在恢复）", cooldown)
                time.sleep(cooldown)
            push(payload, repo, wl_path.name)

        if not args.loop:
            return 0
        log.info("休眠 %d 秒后进入下一轮（Ctrl+C 退出）", args.loop)
        time.sleep(args.loop)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已中断")
        sys.exit(0)
