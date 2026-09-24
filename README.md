# datiya-sub

把 [free.datiya.com](https://free.datiya.com/) 每天发布的免费节点自动聚合，输出一个**地址固定不变**的订阅，
不用再每天换订阅链接。

## 工作原理

```
RSS 发现最近 N 天的文章  →  抓取每天的 uploads/YYYYMMDD-clash.yaml
        ↓
    解析 proxies 并跨天去重（按 server/port/uuid 等连接参数判重）
        ↓
    TCP 连通性检测，剔除已失效的节点
        ↓
    生成 Clash 配置 + v2ray 订阅  →  提交到仓库  →  GitHub Pages 发布
```

站点每天只发布约 14 个节点，其中大部分会滚动替换。默认回溯 **7 天** 一起采集去重，
实测可把候选节点从 14 个提升到 30 个左右，可用性更高。

## 部署（GitHub Actions + Pages）

1. 新建一个仓库（public 或 private 均可），把本项目全部文件推上去。
2. 仓库 **Settings → Actions → General → Workflow permissions** 选择
   **Read and write permissions**（工作流需要提交生成的文件）。
3. 仓库 **Settings → Pages → Build and deployment → Source** 选择 **GitHub Actions**。
   （这一步必须手动做一次，Actions 内置的 `GITHUB_TOKEN` 无权自动开启 Pages。）
4. 到 **Actions → Update subscription → Run workflow** 手动触发一次。
5. 之后每天会自动更新 3 次（北京时间 08:17 / 16:17 / 00:17）。

完成后你的固定订阅地址是：

| 客户端 | 地址 |
| --- | --- |
| Clash / Mihomo / Clash Verge 等 | `https://<用户名>.github.io/<仓库名>/sub/clash.yaml` |
| v2rayN / v2rayNG 等 | `https://<用户名>.github.io/<仓库名>/sub/v2ray.txt` |

把上面地址填进客户端即可长期使用，内容每天自动刷新，地址永远不变。
仓库 Pages 首页（`https://<用户名>.github.io/<仓库名>/`）会显示当前节点数、地区分布和最后更新时间。

## 本地运行

```bash
pip install -r requirements.txt
python main.py                # 采集 + 测活 + 生成到 docs/
python main.py --no-check     # 跳过 TCP 检测（沙箱/无外网环境用）
python main.py --lookback 14  # 回溯 14 天
python main.py --verbose
```

生成结果在 `docs/` 下，用任意静态服务器托管即可（例如 `python -m http.server -d docs 8888`，
订阅地址就是 `http://127.0.0.1:8888/sub/clash.yaml`）。

## 配置说明（config.yaml）

| 配置项 | 说明 |
| --- | --- |
| `site.lookback_days` | 回溯天数，越大节点越多、更新越慢，建议 5~14 |
| `site.retries` / `retry_backoff` | 站点偶尔抖动，失败重试次数与退避 |
| `check.enabled` | 是否做 TCP 连通性检测 |
| `check.timeout` | 单节点 TCP 连接超时，越小越快但可能误杀慢节点 |
| `check.concurrency` | 检测并发数 |
| `check.min_alive` | 可用节点少于该值时**不覆盖**已有订阅，避免把好订阅写坏 |
| `clash.*` | 生成配置的端口、测速地址、测速间隔 |

## 说明与限制

- **TCP 连通性检测 ≠ 真实可用**。它只验证 `server:port` 能否建立连接，能过滤掉大部分已失效的节点，
  但不校验密码/UUID 是否仍然有效。客户端里用 url-test 组会自动挑出当前最快的节点。
- **测活环境决定结果**。GitHub Actions 的机器在境外，检测结果会偏乐观；
  在自己电脑或 VPS 上跑，结果更贴近你的真实网络。
- 站点偶尔返回不完整内容或超时，脚本已内置重试；某天抓取失败不影响其他天。
- 采集失败或可用节点过少时脚本会**直接退出且不修改产物**，订阅不会被清空。

## 免责声明

本项目仅做公开订阅链接的自动聚合与格式转换，供个人学习研究使用。
节点资源来自互联网，请在遵守当地法律法规的前提下使用，禁止用于商业或违法用途。
