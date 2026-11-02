"""白名单条目携带完整节点定义（proxy 字段）的回归测试。

链路：local_probe.probe 采集 -> results[fp]["proxy"] -> merge 写进条目
      -> whitelist.json 推给 CI -> main._rescue_orphans 据此重建候选池。
只给 kbps>0 的条目存定义（零速条目不参与补录，存了白占体积）。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import builder
import local_probe


def _proxy(port):
    return {
        "name": "p0001",
        "type": "vless",
        "server": "1.1.1.1",
        "port": port,
        "uuid": "0000-1111",
        "network": "ws",
        "ws-opts": {"path": "/x"},
    }


def _result(proxy, ok=True, kbps=500.0, with_proxy=True):
    r = {
        "ok": ok,
        "delay": 800,
        "kbps": kbps,
        "type": "vless",
        "line": "%s:%s" % (proxy["server"], proxy["port"]),
        "https": 1,
    }
    if with_proxy:
        r["proxy"] = local_probe._slim_proxy(proxy)
    return r


def test_slim_proxy_drops_internal_fields_and_name():
    p = _proxy(1001)
    p["_speed_kbps"] = 123.0
    p["_srcs"] = ["a"]
    slim = local_probe._slim_proxy(p)
    assert "name" not in slim
    assert "_speed_kbps" not in slim and "_srcs" not in slim
    # 连接参数一个都不能少，否则重建出来的节点连不上
    assert slim["uuid"] == "0000-1111"
    assert slim["ws-opts"]["path"] == "/x"


def test_merge_stores_definition_for_measured_node():
    p = _proxy(1001)
    fp = builder.fingerprint(p)
    entries, _ = local_probe.merge({}, {fp: _result(p)}, keep_days=7, dead_after=3, round_no=1)
    assert entries[fp]["proxy"]["uuid"] == "0000-1111"
    assert entries[fp]["kbps"] == 500.0


def test_merge_drops_definition_when_zero_speed():
    """测通了但测不出网速：不存定义（补录只认 kbps>0）。"""
    p = _proxy(1001)
    fp = builder.fingerprint(p)
    entries, _ = local_probe.merge(
        {}, {fp: _result(p, kbps=0.0)}, keep_days=7, dead_after=3, round_no=1
    )
    assert "proxy" not in entries[fp]


def test_merge_drops_stale_definition_on_failure():
    """已存的旧定义在测失败时清掉：避免 CI 用一份过期参数重建一个连不上的节点。"""
    p = _proxy(1001)
    fp = builder.fingerprint(p)
    old = {"entries": {fp: {"ok_rounds": 3, "proxy": local_probe._slim_proxy(p)}}}
    entries, _ = local_probe.merge(
        old, {fp: _result(p, ok=False, kbps=0.0, with_proxy=False)},
        keep_days=7, dead_after=3, round_no=2,
    )
    assert "proxy" not in entries[fp]


def test_roundtrip_definition_survives_json():
    """白名单要落盘成 JSON 推给 CI，嵌套字段必须原样回来。"""
    import json

    p = _proxy(1001)
    fp = builder.fingerprint(p)
    entries, _ = local_probe.merge({}, {fp: _result(p)}, keep_days=7, dead_after=3, round_no=1)
    back = json.loads(json.dumps({"entries": entries}, ensure_ascii=False))
    assert builder.fingerprint(back["entries"][fp]["proxy"]) == fp
