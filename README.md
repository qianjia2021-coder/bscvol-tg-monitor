# BSC 新币「连续 2 分钟放量 100K」监控

独立部署，不与其它监控任务共享会话、状态或数据。触发条件：**连续两个自然分钟各自成交额 ≥ 100,000 USD**。

- 推送目标：Telegram「BSC推送群」（邀请链接 https://t.me/+0NCSpGbGf_MyNWU1 ，群 id 5194908956）
- 本地目录：`C:\Users\Administrator\bscvol`
- 云端：GitHub `qianjia2021-coder/gmgn-tg-monitor`，工作流 `bsc-vol-monitor`（每 5 分钟）

## 1. 判定规则（严格档）

- 只认**已完整结束**的自然分钟（当前这一分钟不算）
- 连续两个相邻分钟都必须 ≥ 100,000 USD；中间缺一分钟就断开
- 只统计上线后（池子创建时间向下取整到分钟）的分钟
- 每个合约**永久只推一次**（`state.json` 的 `pushed` 落库，且与云端合并）

阈值、连续性、币龄上限都在 `config.json`：

| 键 | 默认 | 含义 |
|---|---|---|
| `threshold_usd` | 100000 | 单分钟成交额阈值 |
| `need_consecutive` | 2 | 需要连续几个分钟达标 |
| `max_age_minutes` | 90 | 只盯上线 90 分钟内的币 |
| `prefilter_m5_usd` | 50000 | DexScreener 5 分钟量预筛门槛（省 Gecko 配额） |
| `watch_tokens` | [] | 手动指定要盯的合约（不受币龄限制） |

## 2. 数据链路

1. **发现**：每 12 秒扫 Pancake V2 工厂 `PairCreated` 事件（`0xca14…0c73`），新池延迟 ≈ 1 个块（BSC 出块约 0.45s）
2. **预筛**：`api.dexscreener.com/latest/dex/tokens/<批量 30 个>` 取 m5 成交额，低于 50K 不进入下一步
3. **判定**：GeckoTerminal 分钟 OHLCV，按池子聚合（保留储备 ≥ 最大池 15% 的池），取每根完成分钟的第 6 个字段（USD 成交额）
4. **推送**：Telethon 用户号 `lu202020`，`parse_mode=html`，合约地址用 `<code>` 可点复制

延迟构成：上线 → 成立 2 分钟窗口 ≈ 120s，Gecko 分钟线落库 10–60s，合计约 **2 分 10 秒–2 分 30 秒**。

## 3. 本机与云端无缝切换

- 本地进程每 180 秒把「心跳 + 去重状态」写回仓库 `bsc_vol_state.json`（单文件，减少提交噪音）
- GitHub Actions 每 5 分钟读该文件：
  - 心跳 < 600 秒 → 判定本机在线，直接 `SKIP`
  - 心跳过期 → 接管：扫最近 6000 块（≈45 分钟）新池、预筛、回看 60 分钟分钟线补推
  - 命中窗口结束超过 15 分钟则不再补推（标 `stale`），避免推过期信号
- 本地启动时会先拉云端 `pushed` 合并，云端推过的不会在本机重推

## 4. 运维命令

```powershell
# 查看实时日志
Get-Content "C:\Users\Administrator\bscvol\bscvol.log" -Tail 20 -Encoding utf8

# 手动启动（无窗口）
wscript.exe "C:\Users\Administrator\bscvol\start_bscvol.vbs"

# 停止
Get-Process pythonw | Where-Object { $_.Path -like "*3.13.12*" } | Stop-Process -Force

# 单独评估某个合约（例如用户给的例子）
python bscvol.py once 0x9665f094d862f4d5c221843e019b463d3c607777

# 回放某币上线首几分钟，验证规则
python bscvol.py replay 0x9665f094d862f4d5c221843e019b463d3c607777 "2026-11-09T03:13:19Z"

# 单元测试
python -m unittest discover -s "C:\Users\Administrator\bscvol" -p "test_*.py"

# 改完代码后同步云端（必须）
python deploy_cloud.py
```

开机自启：`%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\bscvol_monitor.vbs`

## 5. 文件说明

| 文件 | 作用 |
|---|---|
| `bscvol.py` | 内核 + 本地守护（规则、链上发现、Gecko、Telegram、GitHub 同步） |
| `cloud_bscvol.py` | GitHub Actions 接管入口（StringSession，直连无代理） |
| `config.json` | 本机配置（含 api_hash 与 socks5 代理，**不上传仓库**） |
| `config.cloud.json` | 云端配置（无密钥，部署时改名成 `config.json`） |
| `state.json` | 去重与候选状态（原子写） |
| `bscvol.session` | 从 `tg_user.session` 复制的独立会话副本 |
| `test_bscvol.py` | 9 个单元测试（规则/解析/去重） |
| `deploy_cloud.py` | 推送云端 4 个文件到仓库 |
| `trigger_workflow.py` / `fetch_logs.py` | 触发工作流 / 拉取运行日志 |
| `probe_*.py`、`inspect_token.py`、`selftest.py` | 诊断与自检脚本 |

## 6. 已知边界

- 自动发现只覆盖 **Pancake V2** 工厂新池；只建 V3 池的币不会被自动发现，用 `watch_tokens` 手动加入
- GeckoTerminal 免费配额约 30 请求/分钟，预筛门槛调低会放大请求量
- 链上「刷量/对倒」也算成交额，本监控不做地址级去重（如需要可加独立买家数门槛 `txns.m5.buyers` 过滤）
- DNS 对 `api.geckoterminal.com` 有污染，Python 必须走系统代理（`127.0.0.1:7897`，代码自动读注册表）
