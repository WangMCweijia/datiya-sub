"""跨构建存活账本：记录节点「连续通过了几次构建」，用来优待真正长寿的节点。

免费节点寿命以分钟到小时计，单次构建筛出来的「当下最快」往往几分钟后就死。
真正有价值的是那些**能跨多轮构建存活**的少数节点——账本把这件事变成可排序的信号：

- 每轮构建给最终发布的节点 +1「连续存活次数」，本轮被淘汰的节点清零；
- 排序时存活次数高的置顶，同桶内保持传入顺序（已是网速降序）；
- 进入国内可达性探测前按上限截断时，存活次数达标（trusted）的节点无条件占位，
  不会被「额度不够」挤掉——它们是最贵的那批候选。

账本落在仓库根的 JSON 文件里，随 CI 一起提交，所以状态是持久的。
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import builder

log = logging.getLogger("datiya.history")

TZ = timezone(timedelta(hours=8))

# 账本体积控制。每小时一轮、每轮上百个新节点，若不设报废规则，JSON 会无限膨胀。
# 思路：只有「反复出现」的条目值得长期保留；只出现过一次的，过了下几轮就失去意义。
MAX_AGE_DAYS = 7        # streak>=2 的条目：7 天没再出现才删
STALE_HOURS_NEW = 12    # streak==1 的条目：12 小时（约 6 轮）内没再出现就删
STALE_DAYS_FAIL = 3     # streak==0 的淘汰记录：留 3 天供排查
MAX_ENTRIES = 4000      # 硬上限兜底


def _now():
    return datetime.now(TZ)


def _parse_stamp(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S %z")
    except (TypeError, ValueError):
        return None


class Ledger:
    def __init__(self, path, enabled=True):
        self.path = path
        self.enabled = bool(enabled)
        self.data = {"updated_at": None, "nodes": {}}
        if self.enabled:
            self._load()

    def _load(self):
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            log.info("存活账本 %s 不存在，按空账本开始", self.path.name)
            return
        except (OSError, ValueError) as exc:
            log.warning("存活账本读取失败（按空账本继续）：%s", exc)
            return
        nodes = raw.get("nodes") if isinstance(raw, dict) else None
        if isinstance(nodes, dict):
            self.data = {"updated_at": raw.get("updated_at"), "nodes": nodes}
            log.info("存活账本：载入 %d 条历史记录（更新于 %s）",
                     len(nodes), raw.get("updated_at") or "未知")
        else:
            log.warning("存活账本结构异常，按空账本继续")

    # ---- 查询 -------------------------------------------------------------

    def streak(self, proxy):
        if not self.enabled:
            return 0
        rec = self.data["nodes"].get(builder.fingerprint(proxy)) or {}
        try:
            return int(rec.get("streak") or 0)
        except (TypeError, ValueError):
            return 0

    def annotate(self, proxies):
        """给每个节点挂上 _streak（存活次数），供后续排序/展示使用。"""
        if not self.enabled:
            return proxies
        for proxy in proxies:
            proxy["_streak"] = self.streak(proxy)
        return proxies

    def rank(self, proxies, trusted_streak=3):
        """稳定优先：存活次数达标的整体前置，桶内保持速度序（sorted 稳定）。"""
        if not self.enabled:
            return proxies
        trusted = [p for p in proxies if p.get("_streak", 0) >= trusted_streak]
        rest = [p for p in proxies if p.get("_streak", 0) < trusted_streak]
        trusted.sort(key=lambda p: -p.get("_streak", 0))
        return trusted + rest

    def select(self, proxies, limit, trusted_streak=3):
        """按额度截断候选，但存活达标的节点优先占位（不会被额度挤掉）。"""
        if not self.enabled or not limit or len(proxies) <= limit:
            return proxies
        ranked = self.rank(proxies, trusted_streak)
        trusted = [p for p in ranked if p.get("_streak", 0) >= trusted_streak]
        rest = [p for p in ranked if p.get("_streak", 0) < trusted_streak]
        if len(trusted) >= limit:
            if len(trusted) > limit:
                log.warning("存活达标的节点 %d 个已超出探测上限 %d，按存活次数截断",
                            len(trusted), limit)
            return trusted[:limit]
        kept = trusted + rest[: max(0, limit - len(trusted))]
        log.info("候选分层：%d → %d（存活达标 %d 个无条件保留，其余按网速补足）",
                 len(proxies), len(kept), len(trusted))
        return kept

    # ---- 更新 -------------------------------------------------------------

    def commit(self, published, probed, trusted_streak=3):
        """写回账本：发布节点 streak+1，进入测活但被淘汰的清零，久未出现的条目清理。"""
        if not self.enabled:
            return
        now = _now()
        stamp = now.strftime("%Y-%m-%d %H:%M:%S %z")
        nodes = self.data.setdefault("nodes", {})

        published_ids = set()
        for proxy in published:
            fpr = builder.fingerprint(proxy)
            published_ids.add(fpr)
            rec = nodes.setdefault(fpr, {})
            rec["streak"] = int(rec.get("streak") or 0) + 1
            rec["last_seen"] = stamp
            rec["name"] = str(proxy.get("name") or "")[:80]
            rec["kind"] = str(proxy.get("type") or "")
            rec["server"] = str(proxy.get("server") or "")
            rec["port"] = proxy.get("port")
            speed = proxy.get("_speed_kbps")
            if isinstance(speed, (int, float)):
                rec["speed_kbps"] = round(float(speed), 1)
            streak = rec["streak"]
            if streak >= trusted_streak:
                rec["trusted_since"] = rec.get("trusted_since") or stamp

        dropped = 0
        for proxy in probed:
            fpr = builder.fingerprint(proxy)
            if fpr in published_ids:
                continue
            rec = nodes.get(fpr)
            if rec and int(rec.get("streak") or 0) > 0:
                rec["streak"] = 0
                rec["last_fail"] = stamp
                dropped += 1

        keep_cutoff = now - timedelta(days=MAX_AGE_DAYS)
        new_cutoff = now - timedelta(hours=STALE_HOURS_NEW)
        fail_cutoff = now - timedelta(days=STALE_DAYS_FAIL)
        stale = []
        for fpr, rec in nodes.items():
            streak = int(rec.get("streak") or 0)
            last_seen = _parse_stamp(rec.get("last_seen"))
            last_fail = _parse_stamp(rec.get("last_fail"))
            if streak >= 2:
                if last_seen and last_seen < keep_cutoff:
                    stale.append(fpr)
            elif streak == 1:
                if last_seen and last_seen < new_cutoff:
                    stale.append(fpr)
            else:
                reference = last_fail or last_seen
                if not reference or reference < fail_cutoff:
                    stale.append(fpr)
        for fpr in stale:
            nodes.pop(fpr, None)

        pruned = 0
        if len(nodes) > MAX_ENTRIES:
            ordered = sorted(
                nodes.items(),
                key=lambda kv: (int(kv[1].get("streak") or 0),
                                str(kv[1].get("last_seen") or "")),
            )
            for fpr, _ in ordered[: len(nodes) - MAX_ENTRIES]:
                nodes.pop(fpr, None)
                pruned += 1

        self.data["updated_at"] = stamp
        trusted_total = sum(1 for r in nodes.values() if int(r.get("streak") or 0) >= trusted_streak)
        try:
            self.path.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            log.warning("存活账本写入失败（不影响本次发布）：%s", exc)
            return
        log.info("存活账本：记录 %d 条（本次发布 +1 计 %d 个，淘汰清零 %d 个，过期清理 %d 条"
                 "%s，累计连续存活 ≥%d 次的有 %d 个）",
                 len(nodes), len(published_ids), dropped, len(stale),
                 f"，超限再删 {pruned} 条" if pruned else "",
                 trusted_streak, trusted_total)
