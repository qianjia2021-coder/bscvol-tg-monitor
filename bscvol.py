# -*- coding: utf-8 -*-
"""BSC 新币「连续 N 个自然分钟成交额 ≥ 阈值」监控 —— 本地守护 + 云端共用内核
独立运行：自带会话副本 / 独立 state / 独立日志，不读写其它任务的数据。

模块分层：
  规则内核   detect / parse_paircreated / pick_token*   —— 行为由 test_bscvol.py 固定
  基础设施   http / bot_api / github / logging / state
  数据源     Chain(链上扫块) / gecko_* / dex_m5
  判定执行   evaluate_token / build_message / tg_send
  调度       run_local(本机守护) / cloud_bscvol(云端接管)
"""
import base64
import datetime as dt
import gzip
import html
import io
import json
import logging
import logging.handlers
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(HERE, "bscvol.log")
CONFIG_FILE = os.path.join(HERE, "config.json")
STATE_FILE = os.path.join(HERE, "state.json")

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
USDT = "0x55d398326f99059ff775485246999027b3197955"
BUSD = "0xe9e7cea3dedca5984780bafc599bd69add087d56"
USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
FDUSD = "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409"
CAKE = "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
ETH = "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
BTCB = "0x7130d2a12b9bcbae7197e9d0cc42bbb5e3a5f55f"

BASE_TOKENS = {WBNB, USDT, BUSD, USDC, FDUSD, CAKE, ETH, BTCB}

FACTORY_V2 = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
TOPIC_PAIRCREATED = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# 候选不在 Gecko 被收录时的重试间隔（秒），逐级退避，避免浪费配额
NO_POOL_BACKOFF = (60, 120, 240, 480, 900)


# ---------------------------------------------------------------- 规则内核
def detect(minutes, now_ts, threshold, need=2, launch_ts=None):
    """minutes: [(minute_ts, usd), ...]；只认已完整结束的自然分钟。
    返回首个满足「连续 need 分钟各自 ≥ threshold」的窗口。

    不变式（改动前必须先跑通 test_bscvol.py）：
      * 只认 ts + 60 <= now_ts 的自然分钟，进行中的这一分钟不算；
      * 窗口内相邻两分钟必须恰好相差 60 秒，缺一分钟即断开；
      * launch_ts 之前（上线分钟向下取整）的分钟不计入；
      * 阈值为闭区间 v >= threshold。
    """
    done = sorted((ts, float(v)) for ts, v in minutes
                  if int(ts) % 60 == 0 and ts + 60 <= now_ts)
    for i in range(len(done) - need + 1):
        win = done[i:i + need]
        if any(win[j + 1][0] - win[j][0] != 60 for j in range(need - 1)):
            continue
        if launch_ts is not None and win[0][0] < launch_ts:
            continue
        if all(v >= threshold for _, v in win):
            return {
                "minute_start": win[0][0],
                "minute_end": win[-1][0] + 60,
                "vols": [v for _, v in win],
                "vol1": win[0][1],
                "vol2": win[1][1] if need >= 2 else None,
                "sum": sum(v for _, v in win),
            }
    return None


def parse_paircreated(log):
    data = log["data"][2:] if log["data"].startswith("0x") else log["data"]
    return {
        "pair": "0x" + data[0:64][-40:],
        "token0": "0x" + log["topics"][1][-40:],
        "token1": "0x" + log["topics"][2][-40:],
        "block": int(log["blockNumber"], 16),
        "tx": log.get("transactionHash", ""),
    }


def pick_token(pair):
    """交易对里的新币地址（优先非基础币一侧）"""
    cands = pick_token_candidates(pair)
    return cands[0] if cands else None


def pick_token_candidates(pair):
    t0, t1 = pair["token0"].lower(), pair["token1"].lower()
    out = []
    if t0 not in BASE_TOKENS:
        out.append(pair["token0"])
    if t1 not in BASE_TOKENS:
        out.append(pair["token1"])
    if not out:                      # 两个都是基础币（极少见）
        out = [pair["token0"], pair["token1"]]
    return out


def is_pushed(state, token):
    return token.lower() in state.get("pushed", {})


def mark_pushed(state, token, detail, reason="strict"):
    state.setdefault("pushed", {})[token.lower()] = {
        "reason": reason,
        "minute_start": detail.get("minute_start"),
        "vols": detail.get("vols"),
        "at": now_iso(),
    }


# ---------------------------------------------------------------- 调度辅助
def candidate_tier(cand, now, cfg):
    """候选优先级分层（数字越小越先评估）。

    -1 接近达标        —— 已有单根分钟线 ≥ 阈值，只差连续根数；Gecko 结算
                         后极可能立刻命中，最高优先级 + 高频复查
    0 热币 + Gecko 已收录     —— 最可能成立，立刻查
    1 热币 + 尚未收录         —— 币在放量，只是数据源还没跟上
    2 新币(10 分钟内) + 已收录 —— 绕过预筛，但可能还没放量
    3 新币(10 分钟内) + 未收录 —— 兜底
    4 既不热也不新             —— 只等下一轮预筛，不占用 Gecko 配额
    5 Gecko 已收录但池子全老   —— 超出币龄上限，查了也不会成立，不占配额

    剪枝用「历史峰值 m5_peak」而非当前 m5：量能爆发后回落的新币，
    当前 m5 可能已掉到门槛以下，但爆发窗口仍可能落在回看范围内，
    用当前值会把它误判成「永远不可能命中」而漏推。

    注：曾尝试增加「数学上不可能命中」剪枝层（m5 峰值都到不了一根
    分钟线的量就不占配额），但实测 Gecko 配额占用仅 9-14%，
    根本不是瓶颈；剪枝反而让复查流量上升约 36%（净损失）。
    真正影响命中的是阈值与市场量级的匹配，见 README。
    """
    prefilter = float(cfg.get("prefilter_m5_usd", 50000))
    bypass = int(cfg.get("fresh_bypass_minutes", 10)) * 60
    # 峰值优先：m5_peak 记录该币历史上观测到的最大 m5
    m5 = max(float(cand.get("m5") or 0), float(cand.get("m5_peak") or 0))
    hot = m5 >= prefilter
    fresh = (now - int(cand.get("found", now))) <= bypass
    state = cand.get("gecko_state")
    if state == "old":
        # Gecko 收录的池子全部超过币龄上限，判定不可能成立，别再查
        return 5
    if cand.get("near"):
        # 已经有一根分钟线达到阈值，只差连续根数 —— 而 Gecko 的分钟线
        # 要延迟几分钟才结算。此时它极可能马上命中，排到队首最高优先级。
        return -1
    indexed = state == "ok"
    if hot and indexed:
        return 0
    if hot:
        return 1
    if fresh and indexed:
        return 2
    if fresh:
        return 3
    return 4


def next_backoff(step):
    """未收录候选的重试间隔，逐级退避"""
    step = max(0, min(int(step), len(NO_POOL_BACKOFF) - 1))
    return NO_POOL_BACKOFF[step]


def tier4_wait(cand):
    """tier4/tier5 候选（不占 Gecko 配额）的下次检查间隔。

    - 未被 Gecko 收录的候选要沿用自身退避节奏；若一律压回 120 秒，
      退避会被打断，未收录的新币就会每 2 分钟重查一次 Gecko —— 正是配额空烧的根源。
    - 池子已全老（超币龄上限）的候选判定不可能成立，间隔拉长到 5 分钟。
    """
    state = cand.get("gecko_state")
    if state == "old":
        return 300
    if state == "no_pool":
        return next_backoff(cand.get("backoff_step", 0))
    return 120


def min_probe_age(cfg):
    """判定至少需要 need 个完整自然分钟，早于此年龄去查必然查不到，白耗配额"""
    need = int(cfg.get("need_consecutive", 2))
    explicit = cfg.get("min_probe_age_seconds")
    if explicit:
        return int(explicit)
    return need * 60


# ---------------------------------------------------------------- 基础设施
def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def load_state(path=STATE_FILE):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                st = json.load(f)
            backfill_m5_peak(st)
            return st
        except Exception:
            logging.warning("state.json 损坏，按空状态重建")
    return {"pushed": {}, "candidates": {}, "last_block": 0, "target_id": None}


def backfill_m5_peak(state):
    """为老 state 里的候选补 m5_peak 初值。

    m5_peak 记录该币历史上观测到的最大 5 分钟成交额，用途是
    「量能爆发后回落」的新币不被误判为永远不可能命中：
    当前 m5 可能已掉到门槛以下，但爆发窗口仍可能落在回看范围内。

    老 state 没有这个字段，用 DexScreener 的 m5 作初值即可 ——
    它在实测中是 Gecko 的保守下界，偏小只是让剪枝更保守，不会误杀。
    Gecko 评估后会自动回填更大的真实值。
    """
    cands = state.get("candidates") or {}
    n = 0
    for k, v in cands.items():
        if not isinstance(v, dict):
            continue
        if v.get("m5_peak") is None:
            v["m5_peak"] = float(v.get("m5") or 0)
            n += 1
    if n:
        logging.info("回填 m5_peak 初值 %d 个候选", n)


def save_state(state, path=STATE_FILE):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def setup_logging(name="bscvol"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    try:
        # 20MB 滚动，避免日志无限增长；文件名保持 bscvol.log 不变
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=20 * 1024 * 1024, backupCount=2, encoding="utf-8")
    except Exception:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    try:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    except Exception:
        pass
    return logger


def detect_system_proxy():
    """读注册表拿系统代理端口（Clash 混合端口），失败返回 None"""
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
        enable, _ = winreg.QueryValueEx(key, "ProxyEnable")
        server, _ = winreg.QueryValueEx(key, "ProxyServer")
        if enable and server:
            return server if "://" in server else "http://" + server
    except Exception:
        pass
    return None


_opener = None
_opener_proxy = None
_direct_opener = None


def direct_opener():
    """真正不走任何代理的 opener（屏蔽环境变量里的 HTTP_PROXY 等）。

    实测：GitHub API 直连 0.7s 可用，而 Gecko / DexScreener 直连必然超时
    （DNS 污染，见 README），所以只有 GitHub 相关请求适合走这个降级路径。
    """
    global _direct_opener
    if _direct_opener is None:
        _direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _direct_opener


def opener(proxy=None):
    """全局 opener；显式传入 proxy 时按需重建"""
    global _opener, _opener_proxy
    if proxy is None:
        proxy = _opener_proxy if _opener is not None else detect_system_proxy()
    if _opener is None or proxy != _opener_proxy:
        _opener_proxy = proxy
        if proxy:
            _opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        else:
            _opener = urllib.request.build_opener()
    return _opener


def _read_body(resp):
    raw = resp.read()
    if resp.headers.get("Content-Encoding") == "gzip":
        raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
    return raw


def bot_api(token, method, payload=None, timeout=25, tries=3):
    """Telegram Bot API：表单编码 POST（不是 JSON）"""
    url = "https://api.telegram.org/bot%s/%s" % (token, method)
    body = urllib.parse.urlencode(payload or {}).encode()
    h = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": UA}
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers=h, method="POST")
            with opener().open(req, timeout=timeout) as r:
                return json.loads(_read_body(r))
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return json.loads(raw)
            except Exception:
                last = e
        except Exception as e:
            last = e
        time.sleep(0.8 + attempt)
    raise last


def http_json(url, headers=None, data=None, timeout=15, tries=3, proxy=None, method=None,
              allow_direct=False):
    """allow_direct=True 时，代理链路失败会再试一次直连。

    仅对实测直连可用的目标（如 GitHub API）启用；Gecko / DexScreener
    因 DNS 污染直连必然超时，开了反而更慢。
    """
    h = {"Accept": "application/json", "User-Agent": UA, "Accept-Encoding": "gzip"}
    if headers:
        h.update(headers)
    body = json.dumps(data).encode() if data is not None else None
    if body is not None:
        h["Content-Type"] = "application/json"
    if method is None:
        method = "POST" if body is not None else "GET"
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers=h, method=method)
            with opener(proxy).open(req, timeout=timeout) as r:
                raw = _read_body(r)
                # 204 No Content 等场景没有响应体，返回空字典而不是解析失败
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            # 4xx 是确定性错误（未收录 / 不存在），重试没有意义，直接抛出
            if 400 <= e.code < 500 and e.code != 429:
                raise
            last = e
            time.sleep(0.8 + attempt)
        except Exception as e:
            last = e
            time.sleep(0.8 + attempt)
    # 代理链路走不通时，最后再试一次直连（仅对允许直连的目标）
    if allow_direct:
        try:
            req = urllib.request.Request(url, data=body, headers=h, method=method)
            with direct_opener().open(req, timeout=timeout) as r:
                raw = _read_body(r)
                return json.loads(raw) if raw else {}
        except Exception as e:
            last = e
    raise last


# ---------------------------------------------------------------- 链上发现
class Chain:
    def __init__(self, cfg):
        self.rpcs = cfg["rpc"]
        self.i = 0

    def rpc(self, method, params, tries=4):
        last = None
        for attempt in range(tries):
            url = self.rpcs[(self.i + attempt) % len(self.rpcs)]
            try:
                j = http_json(url, data={"jsonrpc": "2.0", "id": 1, "method": method,
                                        "params": params}, timeout=15, tries=1)
                if j.get("error"):
                    raise RuntimeError(j["error"])
                self.i = (self.i + attempt) % len(self.rpcs)
                return j["result"]
            except Exception as e:
                last = e
                time.sleep(0.5)
        raise last

    def block_number(self):
        return int(self.rpc("eth_blockNumber", []), 16)

    def block_ts(self, n):
        return int(self.rpc("eth_getBlockByNumber", [hex(n), False])["timestamp"], 16)

    def _get_logs(self, from_block, to_block):
        """取 PairCreated 日志；节点限制单次区块跨度时自动分片重试。

        实测有节点返回 {"code": -32602, "message": "eth_getLogs is limited to
        0 - 50 blocks range"}。原来把整段一次发出去，一旦轮转到这种节点，
        异常会打断整个主循环（重启后需扫 6000 块时尤其必然触发）。
        """
        params = [{
            "fromBlock": hex(max(0, from_block)), "toBlock": hex(to_block),
            "address": FACTORY_V2, "topics": [TOPIC_PAIRCREATED]}]
        try:
            return self.rpc("eth_getLogs", params)
        except Exception as e:
            if "range" not in str(e).lower():
                raise
        out = []
        step = 50
        for start in range(max(0, from_block), to_block + 1, step):
            end = min(start + step - 1, to_block)
            out.extend(self.rpc("eth_getLogs", [{
                "fromBlock": hex(start), "toBlock": hex(end),
                "address": FACTORY_V2, "topics": [TOPIC_PAIRCREATED]}]))
        return out

    def paircreated(self, from_block, to_block):
        logs = self._get_logs(from_block, to_block)
        return [parse_paircreated(lg) for lg in logs]


# ---------------------------------------------------------------- Gecko
GECKO = "https://api.geckoterminal.com/api/v2/networks/bsc"
_GECKO_GAP = {"t": 0.0}


def gecko_get(url, timeout=18):
    """全局限速：两次请求至少间隔 2.5 秒；遇 429 冷却 25 秒再试一次"""
    for attempt in range(2):
        wait = 2.5 - (time.time() - _GECKO_GAP["t"])
        if wait > 0:
            time.sleep(wait)
        _GECKO_GAP["t"] = time.time()
        try:
            return http_json(url, timeout=timeout, tries=1)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt == 0:
                logging.warning("Gecko 限流，冷却 25 秒")
                time.sleep(25)
                continue
            raise


def gecko_pools(token):
    try:
        j = gecko_get(f"{GECKO}/tokens/{token}/pools")
    except urllib.error.HTTPError as e:
        if e.code == 404:      # 新币尚未被 Gecko 收录
            return []
        raise
    return j.get("data", []) or []


def gecko_minutes(pool_id, limit=10, before_ts=None):
    pid = pool_id.split("_", 1)[1] if pool_id.startswith("bsc_") else pool_id
    url = (f"{GECKO}/pools/{pid}/ohlcv/minute?aggregate=1&limit={limit}&currency=usd")
    if before_ts:
        url += f"&before_timestamp={int(before_ts)}"
    j = gecko_get(url)
    out = []
    for c in ((j.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []:
        out.append((int(c[0]), float(c[5] or 0)))
    return out


def parse_iso(ts):
    if not ts:
        return None
    try:
        return int(dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def evaluate_token(token, cfg, now_ts=None, deep_minutes=None, ignore_age=False,
                   m5_peak=None):
    """返回 (hit, info)；hit 为 detect 结果，info 为展示用信息

    deep_minutes: 回看多少根分钟线。None 时取 config 的 deep_minutes（默认 10）。
      必须大于「发现延迟 + 判定窗口」，否则爆发窗口会滑出回看范围造成漏推。
    m5_peak: 该币历史观测到的最大 5 分钟成交额。用于避免
      「量能爆发后回落」的新币被当前的 low_m5 提前返回挡掉 —— 爆发窗口
      可能仍落在 deep_minutes 回看范围内，必须照常取分钟线。
    """
    now_ts = now_ts or int(time.time())
    if deep_minutes is None:
        deep_minutes = int(cfg.get("deep_minutes", 10))
    max_age = cfg["max_age_minutes"] * 60
    pools = gecko_pools(token)
    if not pools:
        return None, {"state": "no_pool", "pool_count": 0}

    live = []
    for p in pools:
        at = p.get("attributes", {})
        created = parse_iso(at.get("pool_created_at"))
        if created is None:
            continue
        if not ignore_age and now_ts - created > max_age:
            continue
        live.append({
            "id": p["id"], "created": created, "name": at.get("name", ""),
            "reserve": float(at.get("reserve_in_usd") or 0),
            "m5": float(((at.get("volume_usd") or {}).get("m5")) or 0),
            "price": float(at.get("token_price_usd") or at.get("base_token_price_usd") or 0),
            "txns": at.get("transactions") or {},
        })
    if not live:
        # Gecko 收录的池子全部超过币龄上限 —— 判定不可能成立
        return None, {"state": "all_pools_old", "pool_count": len(pools)}

    # low_m5 提前返回：只在「历史上也从没到过门槛」时才放弃。
    # 若该币曾爆发过（m5_peak 够高）而此刻回落，仍要照常取分钟线，
    # 否则爆发窗口会被误杀 —— 这是真实的漏推路径。
    live_m5 = sum(p["m5"] for p in live)
    peak = max(live_m5, float(m5_peak or 0))
    if peak < cfg["prefilter_m5_usd"]:
        return None, {"state": "low_m5", "m5": live_m5,
                      "pool_count": len(live)}

    top = max(p["reserve"] for p in live)
    use = [p for p in live if p["reserve"] >= top * 0.15] or [max(live, key=lambda x: x["reserve"])]

    minutes = {}
    for p in use:
        for ts, v in gecko_minutes(p["id"], limit=deep_minutes):
            minutes[ts] = minutes.get(ts, 0.0) + v
    launch = min(p["created"] for p in live)
    hit = detect([(k, v) for k, v in minutes.items()], now_ts,
                 cfg["threshold_usd"], cfg["need_consecutive"],
                 launch_ts=launch - (launch % 60))
    # 「接近达标」信号：已经有单根分钟线 >= 阈值，但还没凑够连续 need 根。
    #
    # Gecko 的分钟线在结束后还要延迟几分钟才结算出最终值（实测约 3-5 分钟，
    # 且会回填修正）。所以「单根已达标」的币极可能马上就会命中 ——
    # 调度器据此把它排到队首并高频复查，可以显著缩短
    # 「Gecko 结算完成 → 我们发现并推送」这段延迟。
    th = float(cfg["threshold_usd"])
    need = int(cfg.get("need_consecutive", 2))
    done_minutes = [(ts, v) for ts, v in minutes.items() if ts % 60 == 0 and ts + 60 <= now_ts]
    max_one = max((v for _, v in done_minutes), default=0.0)
    # 连续达标根数的最大值（0 表示没有）
    best_run = 0
    run = 0
    for i, (ts, v) in enumerate(sorted(done_minutes)):
        if v >= th:
            run += 1
            best_run = max(best_run, run)
        else:
            run = 0
    near = (not hit) and best_run > 0
    info = {
        "state": "hit" if hit else "watching",
        "pools": use,
        "pool_count": len(live),
        "launch": launch,
        "minutes": minutes,
        "total_vol": sum(minutes.values()),
        "m5": sum(p["m5"] for p in live),
        "reserve": sum(p["reserve"] for p in live),
        "price": max(p["price"] for p in live),
        "name": use[0]["name"],
        "txns": use[0]["txns"],
        "max_one": max_one,
        "best_run": best_run,
        "need": need,
        "near": near,
    }
    return hit, info


# ---------------------------------------------------------------- 电报
def build_message(token, hit, info, cfg):
    m = hit["vols"]
    need = int(cfg.get("need_consecutive", 2))
    launch = info["launch"]
    age_min = int((time.time() - launch) // 60)
    t_txt = time.strftime("%H:%M", time.localtime(launch))
    pool_name = info["name"]
    sym = pool_name.split(" / ")[0].strip() or "?"
    tx = info.get("txns", {}).get("m5", {}) or {}
    buyers = tx.get("buyers")
    total = info.get("total_vol", 0)

    def f(v):
        return f"{v:,.0f}"

    # 池名/符号来自外部数据，必须转义，否则 Telegram 返回 400 导致推送永久失败
    pool_name = html.escape(pool_name, quote=False)
    sym = html.escape(sym, quote=False)

    head = ("🚨 <b>BSC 新币 2 分钟连续放量</b>" if need == 2
            else f"🚨 <b>BSC 新币 {need} 分钟连续放量</b>")
    lines = [
        head,
        f"<b>{sym}</b>　上线 {t_txt}（{age_min} 分钟前）",
        f"合约：<code>{token}</code>",
        f"连续两分钟：{f(m[0])} + {f(m[1])} = <b>{f(sum(m))} USD</b>",
        f"累计 {len(info['minutes'])} 分钟成交：{f(total)} USD",
    ]
    if buyers is not None:
        lines.append(f"近 5 分钟买家 {buyers} 人 / 卖 {tx.get('sellers', '?')} 人")
    lines.append(f"流动性 {f(info['reserve'])} USD　价格 ${info['price']:.8f}".rstrip())
    lines.append(f"池子：{pool_name}")
    lines.append(f'<a href="https://gmgn.ai/bsc/token/{token}">GMGN</a> · '
                 f'<a href="https://dexscreener.com/bsc/{token}">DexScreener</a>')
    return "\n".join(lines)


def tg_client(cfg, session_path):
    from telethon import TelegramClient, functions
    proxy = None
    if cfg.get("tg_proxy"):
        parts = cfg["tg_proxy"].split(":")
        proxy = (parts[0], parts[1], int(parts[2]))
    client = TelegramClient(session_path, cfg["telegram"]["api_id"],
                            cfg["telegram"]["api_hash"], proxy=proxy,
                            connection_retries=5, retry_delay=2,
                            auto_reconnect=True, timeout=30)
    return client, functions


async def tg_ensure_target(client, functions, cfg, state):
    tid = state.get("target_id")
    if tid:
        try:
            return await client.get_entity(tid)
        except Exception:
            state["target_id"] = None
    invite = cfg["telegram"]["invite_hash"]
    try:
        res = await client(functions.messages.ImportChatInviteRequest(invite))
        chats = getattr(res, "chats", []) or []
        if chats:
            state["target_id"] = chats[0].id
            return chats[0]
    except Exception as e:
        name = type(e).__name__
        if "UserAlreadyParticipant" not in name:
            logging.warning("加入群失败 %s: %s", name, e)
    # 已在群里：按标题找
    title = cfg["telegram"].get("group_title", "BSC推送群")
    async for d in client.iter_dialogs(limit=300):
        if d.name and title in d.name:
            state["target_id"] = d.entity.id
            return d.entity
    raise RuntimeError("找不到目标群，且未加入成功")


async def tg_send(client, functions, cfg, state, text):
    bot_token = (cfg.get("bot_token") or os.environ.get("VOL_BOT_TOKEN") or "").strip()
    if bot_token:
        # 目标群一律以 config.json 的 telegram.chat_id 为准，不接受远端旧值
        chat_id = cfg["telegram"].get("chat_id") or state.get("target_id")
        res = bot_api(bot_token, "sendMessage",
                      {"chat_id": chat_id,
                       "text": text, "parse_mode": "html", "disable_web_page_preview": "true"})
        if not res.get("ok"):
            raise RuntimeError("Bot 发送失败: %s" % str(res.get("description"))[:200])
        return None
    if client is None:
        raise RuntimeError("推送通道不可用（无 Bot Token 且用户会话失效）")
    entity = await tg_ensure_target(client, functions, cfg, state)
    await client.send_message(entity, text, parse_mode="html", link_preview=False)
    return entity


# ---------------------------------------------------------------- 云端状态共享
def gh_headers(cfg, token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "User-Agent": "bscvol", "X-GitHub-Api-Version": "2022-11-28"}


def gh_repo(cfg):
    g = cfg["github"]
    return f"https://api.github.com/repos/{g['owner']}/{g['repo']}/contents"


def gh_get_raw(cfg, token, path):
    """读取仓库中的任意文件，返回 (文本, sha)；文件不存在返回 (None, None)"""
    try:
        j = http_json(f"{gh_repo(cfg)}/{path}", headers=gh_headers(cfg, token), timeout=20,
                      allow_direct=True)
        return base64.b64decode(j["content"]).decode("utf-8"), j["sha"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None
        raise
    except Exception:
        return None, None


def gh_get(cfg, token, path):
    """读取仓库中的 JSON 文件（bsc_vol_state.json 等），返回 (对象, sha)"""
    try:
        text, sha = gh_get_raw(cfg, token, path)
        if text is None:
            return None, None
        return json.loads(text), sha
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None
        raise
    except Exception:
        return None, None


def gh_put(cfg, token, path, obj, message, sha=None):
    """写仓库文件。sha 由调用方传入时直接用它做并发校验，省掉一次 GET。"""
    content = base64.b64encode(
        json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8")).decode()
    body = {"message": message, "content": content, "branch": cfg["github"].get("branch", "main")}
    if sha is None:
        _, sha = gh_get(cfg, token, path)
    if sha:
        body["sha"] = sha
    j = http_json(f"{gh_repo(cfg)}/{path}", headers=gh_headers(cfg, token), data=body,
                  timeout=25, tries=3, method="PUT", allow_direct=True)
    return j.get("commit", {}).get("sha")


def newer_hb(a, b):
    """两个心跳 ISO 取较新者；都为空返回 None。共享状态的心跳只增不减。"""
    if not a:
        return b
    if not b:
        return a
    ta, tb = parse_iso(a), parse_iso(b)
    if ta is None or tb is None:
        return b                      # 解析不了时保守采用「远端当前值」
    return a if ta >= tb else b


def merge_shared_state(remote, incoming):
    """写回 bsc_vol_state.json 前，把本端快照与远端当前值合并。

    这个文件是整文件覆盖写入，本机与云端各持一份内存快照（sync_state_down
    只在启动时合并一次）。任何一端直接写自己的快照，都会抹掉对端在运行期间
    新增的 pushed（同一命中被重复推送），或把 hb 写旧（外部触发器反复 dispatch）。
    合并规则：pushed 只增不减、hb 只增不减、identity 跟随较新的心跳。
    """
    remote, incoming = remote or {}, incoming or {}
    pushed = dict(remote.get("pushed") or {})
    for k, v in (incoming.get("pushed") or {}).items():
        pushed.setdefault(k, v)        # 对方已有的记录优先，绝不覆盖或删除
    hb_remote, hb_incoming = remote.get("hb"), incoming.get("hb")
    hb = newer_hb(hb_remote, hb_incoming)
    from_remote = bool(hb_remote) and hb == hb_remote and hb != hb_incoming
    return {
        "hb": hb,
        "identity": ((remote.get("identity") or incoming.get("identity")) if from_remote
                     else (incoming.get("identity") or remote.get("identity"))),
        "pushed": pushed,
        "target_id": incoming.get("target_id") or remote.get("target_id"),
    }


def gh_put_merged(cfg, token, path, obj, message, tries=3):
    """读-合并-写，并用读到的 sha 做 CAS；并发写返回 409 时重读再合并。"""
    last = None
    for attempt in range(tries):
        remote, sha = gh_get(cfg, token, path)
        if remote is None:
            # gh_get 会把网络错误吞成 None（404 也走这里）。先重试读取，
            # 只有连续读不到才退回原写入路径，避免用旧快照覆盖远端记录。
            if attempt + 1 < tries:
                continue
            return gh_put(cfg, token, path, obj, message)
        merged = merge_shared_state(remote, obj)
        try:
            return gh_put(cfg, token, path, merged, message, sha=sha)
        except urllib.error.HTTPError as e:
            if e.code != 409:
                raise
            last = e                   # 期间对方写过，重读再合并
    raise last


def gh_dispatch(cfg, token, workflow="bsc-vol.yml", ref="main", inputs=None):
    """手动触发一次 GitHub Actions 工作流。

    GitHub 的 schedule 事件在新仓库/新 workflow 上可能长期不被调度器纳管
    （实测同账号下 9 个旧 workflow 正常触发，今天新建的 2 个一次没触发）。
    因此除了 cron，还需要一条不依赖调度器的触发路径 —— 本机看护就靠它补位。
    """
    url = f"https://api.github.com/repos/{cfg['github']['owner']}/{cfg['github']['repo']}" \
          f"/actions/workflows/{workflow}/dispatches"
    body = {"ref": ref}
    if inputs:
        body["inputs"] = inputs
    http_json(url, headers=gh_headers(cfg, token), data=body,
              timeout=25, tries=3, method="POST", allow_direct=True)
    # dispatch 成功时 GitHub 返回 204 空体，无需解析
    return True


def gh_last_run_at(cfg, token, workflow="bsc-vol.yml"):
    """最近一次 workflow 运行的创建时间（秒）；没有则返回 0"""
    url = f"https://api.github.com/repos/{cfg['github']['owner']}/{cfg['github']['repo']}" \
          f"/actions/workflows/{workflow}/runs?per_page=1"
    try:
        j = http_json(url, headers=gh_headers(cfg, token), timeout=20, allow_direct=True)
        runs = j.get("workflow_runs") or []
        if not runs:
            return 0
        ts = parse_iso(runs[0].get("created_at"))
        return ts or 0
    except Exception:
        return 0


def cloud_keepalive(cfg, log):
    """本机在线时看护云端：若云端太久没跑，就手动补一次触发。

    背景：GitHub 的 schedule 在新仓库上可能长期不被调度器纳管。本机在线时
    主动补位，保证「云端随时可接管」这一前提成立 —— 本机一旦下线，最近一次
    触发也已经在 10 分钟内，云端能立刻接手。
    """
    if not cfg.get("cloud_keepalive", True):
        return
    token = os.environ.get(cfg["github"]["token_env"], "") or os.environ.get("GITHUB_TOKEN", "")
    if not token:
        return
    max_gap = int(cfg.get("cloud_keepalive_max_gap", 900))
    last = gh_last_run_at(cfg, token)
    now = int(time.time())
    if last and now - last < max_gap:
        return
    gh_dispatch(cfg, token)
    log.info("云端看护：已超过 %d 秒未运行，手动补触发一次", max_gap)


def sync_state_up(cfg, state, heartbeat=True):
    """把心跳 + 去重状态写回仓库（单文件，减少提交噪音）"""
    token = os.environ.get(cfg["github"]["token_env"], "") or os.environ.get("GITHUB_TOKEN", "")
    if not token:
        return False
    payload = {
        "hb": now_iso(),
        "identity": cfg.get("identity", "local"),
        "pushed": state.get("pushed", {}),
        "target_id": state.get("target_id"),
    }
    gh_put_merged(cfg, token, cfg["github"]["state_path"], payload,
                  "bscvol state+heartbeat [skip ci]")
    return True


def heartbeat_now(cfg, state, sync_fn=None, log=None):
    """立刻发布一次心跳，返回是否成功。

    本机重启后需要马上刷心跳：外部触发器（Cloudflare Worker）与云端都靠 hb 判断
    「本机是否在线」。若等到首个主循环走完才写，重启后的头半分钟里两端都会以为
    本机离线 —— 外部触发器会连着 dispatch、云端可能接管，造成重复推送。
    """
    sync_fn = sync_fn or sync_state_up
    try:
        sync_fn(cfg, state, heartbeat=True)
        return True
    except Exception as e:
        if log:
            log.warning("启动心跳写入失败: %s", e)
        return False


def sync_state_down(cfg, state):
    """从仓库拉去重状态并合并（云端推送过的不能在本机重复推）"""
    token = os.environ.get(cfg["github"]["token_env"], "") or os.environ.get("GITHUB_TOKEN", "")
    if not token:
        return 0
    remote, _ = gh_get(cfg, token, cfg["github"]["state_path"])
    if not remote:
        return 0
    local = state.setdefault("pushed", {})
    n = 0
    for k, v in (remote.get("pushed") or {}).items():
        if k not in local:
            local[k] = v
            n += 1
    # 目标群只认本机 config.json，绝不用远端旧值覆盖
    if not cfg["telegram"].get("chat_id") and not state.get("target_id") \
            and remote.get("target_id"):
        state["target_id"] = remote["target_id"]
    return n


def should_push(cfg, state, token_key, log=None):
    """推送前是否仍应推送：先复核共享去重库，再做永久去重判断。

    两端都只在启动时 sync_state_down 一次，运行期间对端推过的币不在自己内存里。
    gh_put_merged 只能保证「写回时不丢记录」，挡不住此刻的并发重复推送。
    读远端失败时 fail-open：宁可冒重复风险，也不能漏掉一条真实命中。
    """
    k = token_key.lower()
    if is_pushed(state, k):
        return False
    try:
        sync_state_down(cfg, state)
    except Exception as e:
        if log:
            log.warning("推送前复核去重库失败（按本地状态继续）: %s", e)
    return not is_pushed(state, k)


MAX_MINUTE_USD = 5_000_000.0   # 单分钟成交额上限，超过即视为数据源异常


def push_guard(hit, recheck, tol=1.5, max_usd=MAX_MINUTE_USD):
    """命中后复核：两次独立读数一致、且量级可信，才允许推送。

    Gecko 在池子刚创建那几分钟偶尔把成交量按 token 计量返回（实测 0xc8fa… 的
    02:39 那根是 200,129,317，下一根却是 110,060），这种荒谬值一旦推出去，
    用户会当成真实信号。复核不一致就暂缓，等下一轮回填修正后再判。
    """
    if not recheck:
        return False
    if int(hit.get("minute_start") or 0) != int(recheck.get("minute_start") or -1):
        return False
    vols1, vols2 = list(hit.get("vols") or []), list(recheck.get("vols") or [])
    if len(vols1) != len(vols2) or not vols1:
        return False
    for a, b in zip(vols1, vols2):
        a, b = float(a or 0), float(b or 0)
        if a > max_usd or b > max_usd:
            return False
        hi, lo = max(a, b), min(a, b)
        if lo <= 0 or hi / lo > tol:
            return False
    return True


# ---------------------------------------------------------------- DexScreener 预筛
DEX = "https://api.dexscreener.com/latest/dex/tokens/"


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def dex_m5(tokens):
    """一次请求最多 30 个地址，返回 {token: m5_usd}（只含有成交数据的地址）"""
    out = {}
    for part in _chunks(list({t.lower() for t in tokens}), 30):
        part_set = set(part)
        try:
            j = http_json(DEX + ",".join(part), timeout=12, tries=2)
        except Exception:
            continue
        for p in j.get("pairs") or []:
            if (p.get("chainId") or "").lower() != "bsc":
                continue
            vs = p.get("volume") or {}
            m5 = float(vs.get("m5") or 0)
            for side in ("baseToken", "quoteToken"):
                a = ((p.get(side) or {}).get("address") or "").lower()
                if a and a in part_set:
                    out[a] = out.get(a, 0.0) + m5
    return {k.lower(): v for k, v in out.items()}


def dex_prefilter(tokens):
    """预筛专用：请求成功的地址一定在返回值里（无成交记 0.0），
    请求失败的地址不在返回值里 —— 调用方据此区分「查过没有量」与「没查成」，
    避免把没查成的候选误判成零成交而永久漏掉。"""
    out = {}
    for part in _chunks(list({t.lower() for t in tokens}), 30):
        part_set = set(part)
        try:
            j = http_json(DEX + ",".join(part), timeout=12, tries=2)
        except Exception:
            continue
        for a in part:
            out.setdefault(a, 0.0)
        for p in j.get("pairs") or []:
            if (p.get("chainId") or "").lower() != "bsc":
                continue
            vs = p.get("volume") or {}
            m5 = float(vs.get("m5") or 0)
            for side in ("baseToken", "quoteToken"):
                a = ((p.get(side) or {}).get("address") or "").lower()
                if a and a in part_set:
                    out[a] = out.get(a, 0.0) + m5
    return out


# ---------------------------------------------------------------- 本地守护
def single_instance(name="bscvol_monitor_mutex"):
    """Windows 命名互斥，防止重复启动两个守护进程"""
    try:
        import ctypes
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = k.CreateMutexW(None, False, name)
        return bool(handle) and ctypes.get_last_error() != 183
    except Exception:
        return True


async def run_local():
    import asyncio
    log = setup_logging()
    if not single_instance():
        log.warning("已有实例在运行，本次退出")
        return
    cfg = load_config()
    state = load_state()
    chain = Chain(cfg)
    bot = (cfg.get("bot_token") or os.environ.get("VOL_BOT_TOKEN") or "").strip()
    client = functions = None
    if bot:
        state["target_id"] = cfg["telegram"].get("chat_id")   # 以配置为准，避免被远端旧值覆盖
        tag = "Bot 模式 chat_id=%s" % state["target_id"]
    else:
        client, functions = tg_client(cfg, os.path.join(HERE, cfg["telegram"]["session"]))
        try:
            await client.start()
            me = await client.get_me()
            entity = await tg_ensure_target(client, functions, cfg, state)
            tag = "账号=%s 目标群=%s" % (getattr(me, "username", me.id),
                                       getattr(entity, "title", entity.id))
        except Exception as e:
            log.error("用户会话不可用（%s: %s），本轮只扫描不推送", type(e).__name__, e)
            client = functions = None
            tag = "扫描模式（推送通道不可用）"
    try:
        merged = sync_state_down(cfg, state)
        log.info("云端去重状态合并 %d 条", merged)
    except Exception as e:
        log.warning("拉取云端状态失败: %s", e)
    save_state(state)
    last_hb = time.time()
    heartbeat_now(cfg, state, log=log)
    log.info("启动：%s", tag)
    while True:
        t0 = time.time()
        try:
            cur = chain.block_number()
            last = int(state.get("last_block") or 0)
            if last <= 0 or cur - last > cfg["max_block_gap"]:
                last = cur - cfg["max_block_gap"]
            pairs = chain.paircreated(last + 1, cur) if cur > last else []
            state["last_block"] = cur
            log.info("扫块 %d-%d 新池 %d 个", last + 1, cur, len(pairs))

            now = int(time.time())
            cands = state.setdefault("candidates", {})
            for t in cfg.get("watch_tokens", []) or []:
                k = t.lower()
                if k not in cands:
                    cands[k] = {"pairs": [], "found": now, "found_block": 0,
                                "next_check": 0, "manual": True}
            for p in pairs:
                for tok in pick_token_candidates(p):
                    k = tok.lower()
                    if k in cands:
                        if p["pair"] not in cands[k]["pairs"]:
                            cands[k]["pairs"].append(p["pair"])
                    else:
                        cands[k] = {"pairs": [p["pair"]], "found": now,
                                    "found_block": p["block"], "next_check": 0}
            # 清理过老候选（手动指定的保留）
            ttl = int(cfg.get("max_age_minutes", 90)) * 60
            for k in list(cands):
                if cands[k].get("manual"):
                    continue
                if now - int(cands[k].get("found", now)) > ttl:
                    cands.pop(k, None)

            due = [k for k, v in cands.items()
                   if not is_pushed(state, k) and now >= int(v.get("next_check", 0))]
            if due:
                # ---- 预筛：轮转覆盖所有候选，而不是只看最新 30 个 ----
                # 原实现只查 due[:30]，老候选永远拿不到 m5，永远进不了 hot 队列，
                # 10 分钟后被清掉 —— 这是漏推的主因。这里按「最久没预筛过」排序，
                # 保证每个候选在一段时间内都能被查到，命中后才进入评估队列。
                dex_refresh = int(cfg.get("dex_refresh_seconds", 45))
                need_dex = [k for k in due
                            if now - int(cands[k].get("dex_at", 0)) >= dex_refresh]
                need_dex.sort(key=lambda k: (int(cands[k].get("dex_at", 0)),
                                             candidate_tier(cands[k], now, cfg)))
                batch = int(cfg.get("prefilter_batch", 90))
                window = need_dex[:batch]
                if window:
                    m5 = dex_prefilter(window)
                    ok = 0
                    for k in window:
                        if k in m5:               # 只更新查成功的，查失败的保留旧值下轮重来
                            cands[k]["dex_at"] = now
                            cands[k]["m5"] = float(m5[k])
                            # 记录历史峰值：量能爆发后可能回落，
                            # 若只看当前值，爆发窗口会被误判成「不可能命中」而漏推
                            if float(m5[k]) > float(cands[k].get("m5_peak") or 0):
                                cands[k]["m5_peak"] = float(m5[k])
                            ok += 1
                    hot_count = sum(1 for k in window
                                    if float(cands[k].get("m5") or 0) >= cfg["prefilter_m5_usd"])
                    log.info("预筛 %d 个候选（成功 %d），热币 %d 个",
                             len(window), ok, hot_count)

                # ---- 分级排队，热点优先 ----
                due.sort(key=lambda k: (candidate_tier(cands[k], now, cfg),
                                        -int(cands[k].get("found", 0))))
                budget = int(cfg.get("eval_per_cycle", 2))
                probe_min_age = min_probe_age(cfg)
                bypass = int(cfg.get("fresh_bypass_minutes", 10)) * 60
                for k in due:
                    c = cands[k]
                    tier = candidate_tier(c, now, cfg)
                    if tier >= 4:
                        # tier4 既不热也不新；tier5 池子全老（超币龄上限）。
                        # 都不占 Gecko 配额，等下一轮预筛。
                        # 保留自身退避节奏（未收录的候选退避可能已达 900s），
                        # 不能一律压回 120s —— 那会打断退避，让未收录的新币
                        # 每 2 分钟就重查一次 Gecko，正是配额空烧的根源。
                        c["next_check"] = now + tier4_wait(c)
                        continue
                    if budget <= 0:
                        # 接近达标的币（tier -1）绝不能被预算挤掉 ——
                        # 它已经有一根分钟线达到阈值，只差 Gecko 结算，
                        # 跳过一轮就是多等一个复查周期。
                        c["next_check"] = now + (6 if tier < 0 else 12)
                        continue
                    age = now - int(c.get("found", now))
                    if age < probe_min_age:
                        # 判定窗口尚未成立，此刻去查必然 no_pool，等够了再来
                        c["next_check"] = now + min(30, max(5, probe_min_age - age))
                        continue
                    budget -= 1
                    hot = max(float(c.get("m5") or 0),
                              float(c.get("m5_peak") or 0)) >= cfg["prefilter_m5_usd"]
                    fresh = age <= bypass
                    c["next_check"] = now + (20 if hot else 60)
                    log.info("评估候选 %s tier=%d m5=%.0f age=%.0f分钟",
                             k, tier, float(c.get("m5") or 0), age / 60.0)
                    try:
                        hit, info = evaluate_token(k, cfg, now,
                                                   ignore_age=bool(c.get("manual")),
                                                   m5_peak=c.get("m5_peak"))
                    except Exception as e:
                        log.warning("评估失败 %s: %s", k, e)
                        # 失败（多为代理 SSL EOF）不该按常规间隔等待 ——
                        # 下一轮就重试，实测可省下 20 秒的窗口。
                        # 但要限流，否则网络持续异常时会打爆 Gecko 配额。
                        fail = int(c.get("fail_count", 0)) + 1
                        c["fail_count"] = fail
                        c["next_check"] = now + min(30, 5 * fail)
                        continue
                    c["fail_count"] = 0
                    log.info("评估 %s -> %s m5=%.0f 分钟数=%d", k, info.get("state"),
                             info.get("m5") or 0, len(info.get("minutes") or {}))
                    # 「接近达标」标记：已有单根分钟线 ≥ 阈值，只差连续根数。
                    # Gecko 的分钟线结束后还要延迟几分钟才结算，此时它极可能
                    # 马上命中 —— 排到队首并高频复查，能显著缩短推送延迟。
                    if info.get("near"):
                        c["near"] = True
                        c["near_best"] = int(info.get("best_run") or 0)
                        c["next_check"] = now + int(cfg.get("near_recheck_seconds", 12))
                        log.info("  接近达标（已连续 %d/%d 根），%d 秒后复查",
                                 c["near_best"], info.get("need"), c["next_check"] - now)
                    else:
                        c["near"] = False
                        c["near_best"] = 0
                    # Gecko 的 m5 比 DexScreener 准（按池子聚合，更贴近链上）。
                    # 必须回写：否则 tier 分层永远用 DexScreener 的旧值，
                    # Gecko 已收录且 m5 很高的币会因 dex_m5=0 落到 tier4 永不评估。
                    g_m5 = float(info.get("m5") or 0)
                    if g_m5 > float(c.get("m5") or 0):
                        c["m5"] = g_m5
                    if g_m5 > float(c.get("m5_peak") or 0):
                        c["m5_peak"] = g_m5
                    if info.get("state") == "no_pool":
                        # 未收录：退避重试，避免每轮白烧一次配额
                        step = int(c.get("backoff_step", 0))
                        c["backoff_step"] = min(step + 1, len(NO_POOL_BACKOFF) - 1)
                        c["gecko_state"] = "no_pool"
                        c["next_check"] = now + next_backoff(c["backoff_step"])
                        continue
                    if info.get("state") == "all_pools_old":
                        # 池子全老（超过币龄上限）：判定不可能成立，
                        # 标记 old 让它退出评估队列，不再反复占用 Gecko 配额
                        c["gecko_state"] = "old"
                        c["next_check"] = now + 300
                        continue
                    c["gecko_state"] = "ok"
                    c["backoff_step"] = 0
                    if hit and should_push(cfg, state, k, log):
                        # 推送前复核一次：挡住 Gecko 在池子初生阶段的异常成交量
                        try:
                            hit2, _ = evaluate_token(k, cfg, now,
                                                     deep_minutes=cfg.get("deep_minutes"),
                                                     ignore_age=True, m5_peak=c.get("m5_peak"))
                        except Exception as e:
                            log.warning("命中复核失败 %s: %s（本轮不推，下轮再判）", k, e)
                            hit2 = None
                        if not push_guard(hit, hit2):
                            log.warning("命中复核不一致，暂缓推送 %s vols=%s 复核=%s", k,
                                        hit.get("vols"), (hit2 or {}).get("vols"))
                            c["next_check"] = now + int(cfg.get("near_recheck_seconds") or 12)
                            save_state(state)
                            continue
                        text = build_message(k, hit, info, cfg)
                        try:
                            await tg_send(client, functions, cfg, state, text)
                        except Exception as e:
                            log.warning("推送失败 %s: %s（保留待推，下轮重试）", k, e)
                            continue
                        mark_pushed(state, k, hit)
                        save_state(state)
                        log.info("已推送 %s 窗口 %s", k,
                                 time.strftime("%H:%M", time.localtime(hit["minute_start"])))
                        try:
                            sync_state_up(cfg, state, heartbeat=True)
                        except Exception as e:
                            log.warning("云端同步失败: %s", e)

            save_state(state)
            if time.time() - last_hb > cfg["heartbeat_seconds"]:
                last_hb = time.time()
                try:
                    sync_state_up(cfg, state, heartbeat=True)
                except Exception as e:
                    log.warning("心跳同步失败: %s", e)
                try:
                    cloud_keepalive(cfg, log)
                except Exception as e:
                    log.warning("云端看护失败: %s", e)

        except Exception as e:
            log.exception("主循环异常: %s", e)
        await asyncio.sleep(max(2, cfg["poll_seconds"] - (time.time() - t0)))


# ---------------------------------------------------------------- 单次/回放（自测用）
async def run_once(token=None):
    """token: 直接评估某合约，打印判定结果与推送文案"""
    log = setup_logging()
    cfg = load_config()
    if token:
        hit, info = evaluate_token(token, cfg)
        print(json.dumps({"hit": hit, "state": info.get("state"),
                          "m5": info.get("m5"), "minutes": sorted(info.get("minutes", {}).items())},
                         ensure_ascii=False, indent=1))
        if hit:
            print("\n" + build_message(token, hit, info, cfg))
        return hit
    return None


async def run_replay(token, launch_iso):
    """回放：用上线后前几分钟的分钟K线验证判定能否触发"""
    cfg = load_config()
    now_ts = parse_iso(launch_iso) + 300
    pools = gecko_pools(token)
    cols = []
    for p in pools:
        created = parse_iso(p["attributes"].get("pool_created_at"))
        if created:
            cols.append((created, p["id"], p["attributes"]["name"],
                         float(p["attributes"].get("reserve_in_usd") or 0)))
    cols.sort()
    if not cols:
        print("没有池子")
        return None
    main = max(cols, key=lambda x: x[3])
    minutes = gecko_minutes(main[1], limit=10, before_ts=now_ts + 60)
    print("pool:", main[2])
    for ts, v in sorted(minutes):
        print("  ", time.strftime("%H:%M", time.localtime(ts)), f"{v:,.0f}")
    hit = detect(minutes, now_ts, cfg["threshold_usd"], cfg["need_consecutive"],
                 launch_ts=parse_iso(launch_iso) // 60 * 60)
    print("判定:", "触发" if hit else "不触发", hit)
    return hit


if __name__ == "__main__":
    import asyncio
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "once":
        asyncio.run(run_once(sys.argv[2] if len(sys.argv) > 2 else None))
    elif mode == "replay":
        asyncio.run(run_replay(sys.argv[2], sys.argv[3]))
    else:
        while True:
            try:
                asyncio.run(run_local())
            except Exception:
                logging.exception("外层异常，5 秒后重连")
                time.sleep(5)
