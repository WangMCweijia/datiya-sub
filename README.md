# datiya-sub

把 [free.datiya.com](https://free.datiya.com/) 每天发布的免费节点自动聚合，输出一个**地址固定不变**的订阅，
不用再每天换订阅链接。

## 工作原理

```
free.datiya.com：RSS 发现最近 N 天的文章  →  抓取每天的 uploads/YYYYMMDD-clash.yaml
        +
附加源：GitHub 上若干公开免费节点订阅（Clash 格式），见 config.yaml 的 sources
        ↓
    合并后统一解析 proxies 并去重（按 server/port/uuid 等连接参数判重）
        ↓
    清洗节点（剔除明文 http/socks5、内核不支持的协议）
        ↓
    第一轮：mihomo 内核真实连通性 + 延迟（超 max_delay_ms 的剔除）
        ↓
    第二轮：经每个节点真实下载测速（网速低于 min_speed_kbps 的剔除）
        ↓
    生成 Clash 配置 + v2ray 订阅  →  提交到仓库  →  GitHub Pages 发布
```

站点每天只发布约 14 个节点，单靠它候选太少。默认回溯 **7 天**，并额外合并若干 GitHub 公开订阅源，
候选可提升到近千个；再经真实测速，留下的是确实能代理出网的节点。

### 附加源（sources）

`config.yaml` 的 `sources` 列出若干公开的免费节点订阅（Clash 格式）。脚本**只读取每个源里的
`proxies` 列表**，源自身的 `proxy-groups` / `rules` / `dns` 等一律忽略，因此不会把第三方配置
带进你的订阅里。增删源只需改这个列表：

```yaml
sources:
  - name: ripaojiedian
    url: "https://raw.githubusercontent.com/ripaojiedian/freenode/main/clash"
  # ... 其它源
```

某个源当天抓取失败只会被跳过并记 0，不影响其它源。

## 部署（GitHub Actions + Pages）

1. 新建一个仓库（public 或 private 均可），把本项目全部文件推上去。
2. 仓库 **Settings → Actions → General → Workflow permissions** 选择
   **Read and write permissions**（工作流需要提交生成的文件）。
3. 仓库 **Settings → Pages → Build and deployment → Source** 选择 **GitHub Actions**。
   （这一步必须手动做一次，Actions 内置的 `GITHUB_TOKEN` 无权自动开启 Pages。）
4. 到 **Actions → Update subscription → Run workflow** 手动触发一次。
5. 之后每天会自动更新 3 次（北京时间 08:17 / 16:17 / 00:17）；此外，任何推送到 `main`
   且改动了 `docs/` 以外文件的提交（例如调整 `config.yaml` 的 `sources`）也会自动触发一次刷新。

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
python main.py                # 采集 + 真实测速 + 生成到 docs/
python main.py --no-check     # 跳过测活（沙箱/无外网环境用）
python main.py --lookback 14  # 回溯 14 天
python main.py --verbose
```

默认用 [mihomo](https://github.com/MetaCubeX/mihomo)（clash.meta）内核做真实测速，本地运行需要它：
放到 `PATH` 里，或用 `check.binary` 指定路径（例如 `/tmp/mihomo`）。没有内核时会自动回退为 TCP 检测。

生成结果在 `docs/` 下，用任意静态服务器托管即可（例如 `python -m http.server -d docs 8888`，
订阅地址就是 `http://127.0.0.1:8888/sub/clash.yaml`）。

## 配置说明（config.yaml）

| 配置项 | 说明 |
| --- | --- |
| `site.lookback_days` | 回溯天数，越大节点越多、更新越慢，建议 5~14 |
| `site.retries` / `retry_backoff` | 站点偶尔抖动，失败重试次数与退避 |
| `sources` | 附加的公开订阅源列表（Clash 格式），只取其中的 `proxies`；可自由增删 |
| `check.enabled` | 是否做测活 |
| `check.method` | `mihomo`（用内核真实测速，推荐）或 `tcp`（仅测端口连通，精度低） |
| `check.test_url` / `timeout_ms` | 第一轮连通性测试的目标地址与单节点超时（毫秒） |
| `check.max_delay_ms` | 第一轮延迟上限，超过的剔除；`0` 表示不限 |
| `check.speed_url` / `speed_bytes` | 第二轮下载测速的地址与下载字节预算 |
| `check.speed_timeout_ms` | 第二轮单节点测速时间预算（毫秒） |
| `check.speed_concurrency` | 第二轮并发数，太高会互相抢带宽导致测不准 |
| `check.min_speed_kbps` | 网速下限（KB/s），低于的剔除 |
| `check.concurrency` | 第一轮并发测速数 |
| `check.min_alive` | 可用节点少于该值时**不覆盖**已有订阅，避免把好订阅写坏 |
| `clash.*` | 生成配置的端口、测速地址、测速间隔 |

## 说明与限制

- **默认做两轮真实测速**：第一轮把候选节点灌进 mihomo 内核逐个请求 `generate_204`，
  拿不到响应的（协议/凭据失效、无法出网）直接剔除，再按 `max_delay_ms` 卡掉高延迟节点；
  第二轮给每个节点单独开一个入站端口，**经该节点真实下载** `speed_url` 并按「字节/耗时」
  算出网速，低于 `min_speed_kbps` 的剔除。测速前会先剔除明文 `http`/`socks5` 代理和内核不支持的协议。
- **仍不能保证 100% 可用**。免费节点寿命很短，几分钟就可能失效；且测活机在境外，
  与你的网络环境不同，能测通的节点你未必连得上。客户端里用 url-test 组会自动挑当前最快的。
- 站点偶尔返回不完整内容或超时，脚本已内置重试；某天抓取失败不影响其他天。
- 采集失败或可用节点过少时脚本会**直接退出且不修改产物**，订阅不会被清空。

## 免责声明

本项目仅做公开订阅链接的自动聚合与格式转换，供个人学习研究使用。
节点资源来自互联网，请在遵守当地法律法规的前提下使用，禁止用于商业或违法用途。
