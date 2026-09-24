"""节点 TCP 连通性检测。"""

import asyncio
import logging

log = logging.getLogger("datiya.check")


async def _probe(sem, host, port, timeout):
    async with sem:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout
            )
        except Exception:
            return False
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True


async def _gather(targets, timeout, concurrency):
    sem = asyncio.Semaphore(concurrency)
    return await asyncio.gather(*(_probe(sem, h, p, timeout) for h, p in targets))


def filter_alive(proxies, timeout=3.0, concurrency=200):
    """保留 server:port 可建立 TCP 连接的节点。"""
    targets = []
    kept = []
    for proxy in proxies:
        try:
            targets.append((str(proxy["server"]), int(proxy["port"])))
            kept.append(proxy)
        except (KeyError, TypeError, ValueError):
            log.debug("跳过字段异常的节点: %s", proxy)
    if not targets:
        return []

    results = asyncio.run(_gather(targets, timeout, concurrency))
    alive = [proxy for proxy, ok in zip(kept, results) if ok]
    log.info("连通性检测：%d/%d 个节点可连接", len(alive), len(kept))
    return alive