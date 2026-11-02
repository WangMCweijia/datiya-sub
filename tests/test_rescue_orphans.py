"""候选池白名单补录（main._rescue_orphans）的回归测试。

背景：候选池每轮从本轮采集结果重建，节点某轮没被源站采到就彻底出局——
不进池 → 不重测 → round 停旧值 → whitelist.apply 判 stale 下线。
第 43 轮实测：白名单 998 条里 294 条有网速，只测到 169 条，125 个好节点白扔。
"""

import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import builder
import main


def _proxy(port, name="p"):
    return {
        "name": name,
        "type": "vless",
        "server": "1.1.1.1",
        "port": port,
        "uuid": "0000-1111",
        "network": "ws",
        "ws-opts": {"path": "/x"},
    }


def _write_prev(root, proxies):
    items = []
    for p in proxies:
        item = {k: v for k, v in p.items() if not k.startswith("_") and k != "name"}
        item["_fp"] = builder.fingerprint(p)
        items.append(item)
    blob = gzip.compress(
        json.dumps({"version": 1, "proxies": items}, ensure_ascii=False).encode(), 9
    )
    (root / "candidates.json.gz").write_bytes(blob)


def _run(tmp_path, rest, entries, prev):
    root = Path(tmp_path)
    _write_prev(root, prev)
    old_root = main.ROOT
    main.ROOT = root
    try:
        return main._rescue_orphans(rest, entries, {"file": "candidates.json.gz"})
    finally:
        main.ROOT = old_root


def test_rescues_whitelisted_node_missing_from_sources(tmp_path):
    alive = _proxy(1001)
    fp_alive = builder.fingerprint(alive)
    out, n = _run(tmp_path, [], {fp_alive: {"ok_rounds": 3, "last_ok": "x"}}, [alive])
    assert n == 1
    assert [builder.fingerprint(p) for p in out] == [fp_alive]
    # name 被 write_candidates 剥掉，补录必须自己填一个，否则 builder 会 KeyError
    assert out[0]["name"].startswith("rescued-")


def test_skips_node_already_in_rest(tmp_path):
    alive = _proxy(1001)
    fp = builder.fingerprint(alive)
    out, n = _run(tmp_path, [alive], {fp: {"ok_rounds": 1}}, [alive])
    assert n == 0
    assert len(out) == 1


def test_skips_node_not_in_whitelist(tmp_path):
    """> 不在白名单的节点不补录——那是「未验证」组的事，补录只救已验证的。"""
    p = _proxy(1001)
    out, n = _run(tmp_path, [], {}, [p])
    assert n == 0
    assert out == []


def test_skips_dead_entry(tmp_path):
    p = _proxy(1001)
    fp = builder.fingerprint(p)
    out, n = _run(tmp_path, [], {fp: {"ok_rounds": 0, "dead": True}}, [p])
    assert n == 0


def test_rescued_go_first_so_quota_cannot_starve_them(tmp_path):
    """补录节点排在 rest 最前：split_verified 之后按 last_ok 排，
    但同新鲜度下先到先得，放前面保证它们不被新节点挤掉顺序。"""
    a, b = _proxy(1001), _proxy(1002)
    entries = {builder.fingerprint(a): {"ok_rounds": 1}, builder.fingerprint(b): {"ok_rounds": 1}}
    out, n = _run(tmp_path, [b], entries, [a, b])
    assert n == 1
    assert builder.fingerprint(out[0]) == builder.fingerprint(a)


def test_missing_prev_file_is_not_fatal(tmp_path):
    root = Path(tmp_path)
    old_root = main.ROOT
    main.ROOT = root
    try:
        out, n = main._rescue_orphans([], {"x": {}}, {"file": "nope.json.gz"})
    finally:
        main.ROOT = old_root
    assert (out, n) == ([], 0)


def test_plain_json_prev_pool_also_works(tmp_path):
    """候选池可能是未压缩的 .json（candidates.file 可配）。"""
    p = _proxy(1001)
    fp = builder.fingerprint(p)
    root = Path(tmp_path)
    item = {k: v for k, v in p.items() if not k.startswith("_") and k != "name"}
    (root / "candidates.json").write_text(
        json.dumps({"version": 1, "proxies": [item]}, ensure_ascii=False), encoding="utf-8"
    )
    old_root = main.ROOT
    main.ROOT = root
    try:
        out, n = main._rescue_orphans([], {fp: {"ok_rounds": 1}}, {"file": "candidates.json"})
    finally:
        main.ROOT = old_root
    assert n == 1


def test_corrupt_prev_pool_falls_back_to_empty(tmp_path):
    root = Path(tmp_path)
    (root / "candidates.json.gz").write_bytes(b"not gzip at all")
    old_root = main.ROOT
    main.ROOT = root
    try:
        out, n = main._rescue_orphans([], {"x": {}}, {"file": "candidates.json.gz"})
    finally:
        main.ROOT = old_root
    assert (out, n) == ([], 0)
