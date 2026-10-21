# datiya-sub

把 [free.datiya.com](https://free.datiya.com/) 每天发布的免费节点自动聚合，输出一个**地址固定不变**的订阅，
不用再每天换订阅链接。

## 工作原理

```
free.datiya.com：RSS 发现最近 N 天的文章  →  抓取每天的 uploads/YYYYMMDD-clash.yaml
        +
附加源：GitHub 上若干公开免费节点订阅（Clash 格式），见 config.yaml 的 sources
        ↓
    合并后统一解析 proxies 并去重（按「连接身份」= 协议/服务器/端口/凭据/SNI/传输）
        ↓
    清洗节点（剔除内核不支持的协议、明文 http/socks5 代理、字段缺失/端口异常的节点）
        ↓
    第一轮：mihomo 内核真实连通性 + 延迟（超 max_delay_ms 的剔除）
        ↓
    第二轮：经每个节点真实下载测速（网速低于 min_speed_kbps 的剔除）
        ↓
    存活账本：标注每个节点「连续通过了几次构建」→ 长寿节点置顶 → 按探测额度截断
        ↓
    第三轮：中国大陆可达性（Globalping 从国内探测点验证，只保留国内连得上的）
        ↓
    发布前校验：mihomo -t 加载生成的配置，不通过则报错并保留原有订阅
        ↓
    生成 Clash 配置 + v2ray 订阅  →  记账本  →  提交到仓库  →  GitHub Pages 发布
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
5. 之后会**每 2 小时**自动更新一次（UTC 偶数小时 17 分，即北京时间 10:17 / 12:17 / …）；
   此外，任何推送到 `main` 且改动了 `docs/` 以外文件的提交（例如调整 `config.yaml` 的
   `sources`）也会自动触发一次刷新。之所以不设每小时：单次构建要消耗约 226 个
   Globalping tests，而免费额度是 250/h，间隔 1 小时会贴着限额跑、容易触发 429。
   配了 `GLOBALPING_TOKEN`（额度 500/h）后可以把 cron 改成 `17 * * * *`。

完成后你的固定订阅地址是：

| 客户端 | 地址 |
| --- | --- |
| Clash / Mihomo / Clash Verge 等 | `https://<用户名>.github.io/<仓库名>/sub/clash.yaml` |
| v2rayN / v2rayNG 等 | `https://<用户名>.github.io/<仓库名>/sub/v2ray.txt` |

把上面地址填进客户端即可长期使用，内容每天自动刷新，地址永远不变。
仓库 Pages 首页（`https://<用户名>.github.io/<仓库名>/`）会显示当前节点数、协议构成、
独立线路数与唯一率、存活轮次分布、地区分布和最后更新时间。

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
| `check.china_check.enabled` | 第三轮：是否做中国大陆可达性检测（借助 Globalping 公开 API） |
| `check.china_check.mode` | `strict`=只保留国内连得上的（激进，节点更少更干净）；`safe`=只剔除确认被墙的（保守） |
| `check.china_check.country` | 探测点所在国家，默认 `CN`（中国大陆） |
| `check.china_check.reference_country` | 仅 `safe` 模式使用的复核探测点所在国家，默认 `US` |
| `check.china_check.limit` | 每个「服务器:端口」用几个探测点，免费额度约 250 次/小时，慎调大 |
| `check.china_check.max_units` | 进入国内探测的节点数上限（默认 240，`0` 不限）。额度不够时「留下谁」会变成「先测谁」，这里按「存活次数 → 网速」截断，把额度花在最值得的候选上 |
| `check.min_alive` | 可用节点少于该值时**不覆盖**已有订阅（默认 30，避免把好订阅写坏） |
| `history.enabled` / `history.file` | 跨构建存活账本：开关与文件路径（默认仓库根 `history.json`） |
| `history.trusted_streak` | 连续通过几次构建算「可信」（默认 3）：置顶排序 + 探测额度不足时无条件保留 |
| `clash.*` | 生成配置的端口、测速地址、测速间隔 |

## 说明与限制

- **协议白名单**：只保留 `ss` / `vmess` / `vless` / `trojan` / `hysteria2` / `hysteria` /
  `anytls`。`http` / `socks5` 明文代理整类剔除——实测公开源里这类节点数量极大
  （曾占到发布订阅的一半），TCP/TLS 层全都「活着」（只是开着 443/9002 端口并能完成 TLS
  握手），但真实代理请求全部被拒，抽样可用率仅 4%，既污染订阅又白耗探测额度。
  需要保留可把它们加回 `builder.CORE_TYPES`。
- **去重按连接身份**：key = 协议 + 服务器 + 端口 + 凭据 + SNI + 传输方式，同一线路的多份
  包装（只差 `udp`、`client-fingerprint`、`sub_tag` 之类附属字段）合并成一个节点并补全
  缺失字段。旧实现拿「除名字外的全部字段」做 key，实测会让同一 `server:port` 在订阅里
  出现 5~6 份（237 个节点其实只有 148 条线路）。
- **存活账本**（`history.json`）：每轮给最终发布的节点「连续通过次数」+1，本轮被淘汰的
  清零，只出现过一次的条目 12 小时后清理（账本不会无限膨胀）。排序时长寿节点置顶，
  且探测额度不足时无条件占位——免费节点里能跨多轮构建存活的极少数才是真金，
  不该被「当轮最快」的新节点挤掉。见 `history.py`。
- **探测单位包含服务器本身**：第三轮的单位身份是「服务器 + 端口 + SNI + Host + 路径」。
  只用 SNI 作身份会把「同一 CDN 域名背后不同 IP」的节点折成一个单位，实测曾出现
  一个单位代表 76 个节点（占订阅 32%）——那一次探测通过就全部放行，反之整批删掉。
- **v2ray 订阅**覆盖 `ss` / `vmess` / `vless`（含 reality 的 `pbk`/`sid`/`spx`/`fp`、
  gRPC 的 `serviceName`）/ `trojan` / `hysteria2` / `anytls`，与 Clash 侧节点数一致。
- **默认做三轮筛选**：第一轮把候选节点灌进 mihomo 内核逐个请求 `generate_204`，
  拿不到响应的（协议/凭据失效、无法出网）直接剔除，再按 `max_delay_ms` 卡掉高延迟节点；
  第二轮给每个节点单独开一个入站端口，**经该节点真实下载** `speed_url` 并按「字节/耗时」
  算出网速，低于 `min_speed_kbps` 的剔除。测速前会先剔除内核不支持的协议与字段缺失/端口异常的节点。
- **第三轮补上「国内视角」**：前两轮都在 GitHub 的境外机器上跑，能测通不等于你在国内连得上。
  第三轮借助 [Globalping](https://globalping.io/) 公开 API，用中国大陆的探测点对每个节点的
  **服务端口**做 TCP 拨号。只 ping 主机是不够的——很多运营商放行 ICMP 却封掉代理的真实端口，
  于是「ping 通、端口不通」的节点会被保留、导入后一测就失效。
  TLS 节点会从国内家宽探测点发真实 HTTPS 请求（SNI / Host / 路径都取节点自身的配置），
  因为 GFW 拦的是 SNI、CDN 按 SNI 路由；CDN 边缘对任何握手都应答，所以额外检查响应语义：
  边缘返回 5xx（522/523 等）或「Worker 不存在 / 源站不可达」页面时判死，而代理服务器对
  普通 GET 回 400/503 属正常，不据此判死。默认 `mode: strict`（激进）：
  **国内连得上才保留**，不通或结果未知的一律剔除，节点更少但更干净。想更保守可改成
  `mode: safe`：只剔除「国内不通但换境外探测点能连上」的确认被墙节点，两地都不通的多半是
  探测协议不匹配，无法据此判断，一律保留（几乎不误杀）。
  注意 hysteria2 等 UDP/QUIC 协议无法用 TCP 拨号验证，这些节点退回 ICMP 判断，属尽力而为。
  接口限速或异常导致**全部**探测结果未知时会自动跳过该过滤，不会因为第三方抖动把订阅写空。
  免费额度约 250 次/小时，探测名额不够时按延迟从低到高优先探测快节点，被挤掉的只会是慢节点。
  不想用可把 `check.china_check.enabled` 设为 `false`。
- **仍不能保证 100% 可用**。免费节点寿命很短，几分钟就可能失效；测活机在境外，
  与你的网络环境不同。客户端里用 url-test 组（默认的「♻️ 自动选择」）会自动挑当前最快的，
  每隔几分钟重测一次并自动切换，不会一直卡在死节点上。
- 站点偶尔返回不完整内容或超时，脚本已内置重试；某天抓取失败不影响其他天。
- 采集失败或可用节点过少时脚本会**直接退出且不修改产物**，订阅不会被清空。

## 免责声明

本项目仅做公开订阅链接的自动聚合与格式转换，供个人学习研究使用。
节点资源来自互联网，请在遵守当地法律法规的前提下使用，禁止用于商业或违法用途。
