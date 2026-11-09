# -*- coding: utf-8 -*-
"""BSC 新币「连续 N 个自然分钟成交额 ≥ 阈值」监控 —— 本地守护 + 云端共用内核
独立运行：自带会话副本 / 独立 state / 独立日志，不读写其它任务的数据。
"""
import base64
import datetime as dt
import gzip
import io
import json
import logging
import os
import sys
import time
import urllib.error
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


# ---------------------------------------------------------------- 规则内核
def detect(minutes, now_ts, threshold, need=2, launch_ts=None):
    """minutes: [(minute_ts, usd), ...]；只认已完整结束的自然分钟。
    返回首个满足「连续 need 分钟各自 ≥ threshold」的窗口。"""
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
                return json.load(f)
        except Exception:
            logging.warning("state.json 损坏，按空状态重建")
    return {"pushed": {}, "candidates": {}, "last_block": 0, "target_id": None}


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


def opener(proxy=None):
    global _opener
    if _opener is None:
        proxy = proxy or detect_system_proxy()
        if proxy:
            _opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        else:
            _opener = urllib.request.build_opener()
    return _opener


def http_json(url, headers=None, data=None, timeout=15, tries=3, proxy=None, method=None):
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
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return json.loads(raw)
        except Exception as e:
            last = e
            time.sleep(0.8 + attempt)
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

    def paircreated(self, from_block, to_block):
        logs = self.rpc("eth_getLogs", [{
            "fromBlock": hex(max(0, from_block)), "toBlock": hex(to_block),
            "address": FACTORY_V2, "topics": [TOPIC_PAIRCREATED]}])
        return [parse_paircreated(lg) for lg in logs]


# ---------------------------------------------------------------- Gecko
GECKO = "https://api.geckoterminal.com/api/v2/networks/bsc"


def gecko_pools(token):
    j = http_json(f"{GECKO}/tokens/{token}/pools", timeout=18)
    return j.get("data", []) or []


def gecko_minutes(pool_id, limit=10, before_ts=None):
    pid = pool_id.split("_", 1)[1] if pool_id.startswith("bsc_") else pool_id
    url = (f"{GECKO}/pools/{pid}/ohlcv/minute?aggregate=1&limit={limit}&currency=usd")
    if before_ts:
        url += f"&before_timestamp={int(before_ts)}"
    j = http_json(url, timeout=18)
    out = []
    for c in (j.get("data", {}).get("attributes", {}) or {}).get("ohlcv_list", []) or []:
        out.append((int(c[0]), float(c[5] or 0)))
    return out


def parse_iso(ts):
    if not ts:
        return None
    try:
        return int(dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def evaluate_token(token, cfg, now_ts=None, deep_minutes=10, ignore_age=False):
    """返回 (hit, info)；hit 为 detect 结果，info 为展示用信息"""
    now_ts = now_ts or int(time.time())
    max_age = cfg["max_age_minutes"] * 60
    pools = gecko_pools(token)
    if not pools:
        return None, {"state": "no_pool"}

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
        return None, {"state": "all_pools_old"}

    if sum(p["m5"] for p in live) < cfg["prefilter_m5_usd"]:
        return None, {"state": "low_m5", "m5": sum(p["m5"] for p in live)}

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
    info = {
        "state": "hit" if hit else "watching",
        "pools": use,
        "launch": launch,
        "minutes": minutes,
        "total_vol": sum(minutes.values()),
        "m5": sum(p["m5"] for p in live),
        "reserve": sum(p["reserve"] for p in live),
        "price": max(p["price"] for p in live),
        "name": use[0]["name"],
        "txns": use[0]["txns"],
    }
    return hit, info


# ---------------------------------------------------------------- 电报
def build_message(token, hit, info, cfg):
    m = hit["vols"]
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

    lines = [
        "🚨 <b>BSC 新币 2 分钟连续放量</b>",
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


def gh_get(cfg, token, path):
    try:
        j = http_json(f"{gh_repo(cfg)}/{path}", headers=gh_headers(cfg, token), timeout=20)
        return json.loads(base64.b64decode(j["content"]).decode("utf-8")), j["sha"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None
        raise
    except Exception:
        return None, None


def gh_put(cfg, token, path, obj, message):
    content = base64.b64encode(
        json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8")).decode()
    body = {"message": message, "content": content, "branch": cfg["github"].get("branch", "main")}
    _, sha = gh_get(cfg, token, path)
    if sha:
        body["sha"] = sha
    j = http_json(f"{gh_repo(cfg)}/{path}", headers=gh_headers(cfg, token), data=body,
                  timeout=25, tries=3, method="PUT")
    return j.get("commit", {}).get("sha")


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
    gh_put(cfg, token, cfg["github"]["state_path"], payload,
           "bscvol state+heartbeat [skip ci]")
    return True


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
    if not state.get("target_id") and remote.get("target_id"):
        state["target_id"] = remote["target_id"]
    return n


# ---------------------------------------------------------------- DexScreener 预筛
DEX = "https://api.dexscreener.com/latest/dex/tokens/"


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def dex_m5(tokens):
    """一次请求最多 30 个地址，返回 {token: m5_usd}"""
    out = {}
    uniq = list({t.lower() for t in tokens})
    for part in _chunks(uniq, 30):
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
                if a and a in part:
                    out[a] = out.get(a, 0) + m5
    return {k.lower(): v for k, v in out.items()}


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
    client, functions = tg_client(cfg, os.path.join(HERE, cfg["telegram"]["session"]))
    await client.start()
    me = await client.get_me()
    entity = await tg_ensure_target(client, functions, cfg, state)
    try:
        merged = sync_state_down(cfg, state)
        log.info("云端去重状态合并 %d 条", merged)
    except Exception as e:
        log.warning("拉取云端状态失败: %s", e)
    save_state(state)
    log.info("启动：账号=%s 目标群=%s", getattr(me, "username", me.id), getattr(entity, "title", entity.id))

    last_hb = 0.0
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
                        cands[k]["pairs"].append(p["pair"])
                    else:
                        cands[k] = {"pairs": [p["pair"]], "found": now,
                                    "found_block": p["block"], "next_check": 0}
            # 清理过老候选（手动指定的保留）
            for k in list(cands):
                if cands[k].get("manual"):
                    continue
                if now - int(cands[k].get("found", now)) > cfg["max_age_minutes"] * 60:
                    cands.pop(k, None)

            due = [k for k, v in cands.items()
                   if not is_pushed(state, k) and now >= int(v.get("next_check", 0))]
            if due:
                m5 = dex_m5(due[:30])
                for k in due:
                    cands[k]["next_check"] = now + 20
                    if m5.get(k, 0) < cfg["prefilter_m5_usd"]:
                        continue
                    log.info("预筛命中 %s m5=%.0f", k, m5.get(k, 0))
                    try:
                        hit, info = evaluate_token(k, cfg, now,
                                                   ignore_age=bool(cands[k].get("manual")))
                    except Exception as e:
                        log.warning("评估失败 %s: %s", k, e)
                        continue
                    log.info("评估 %s -> %s m5=%.0f 分钟数=%d", k, info.get("state"),
                             info.get("m5") or 0, len(info.get("minutes") or {}))
                    if hit and not is_pushed(state, k):
                        text = build_message(k, hit, info, cfg)
                        await tg_send(client, functions, cfg, state, text)
                        mark_pushed(state, k, hit)
                        save_state(state)
                        log.info("已推送 %s 窗口 %s", k,
                                 time.strftime("%H:%M", time.localtime(hit["minute_start"])))
                        try:
                            sync_state_up(cfg, state, heartbeat=True)
                        except Exception as e:
                            log.warning("云端同步失败: %s", e)
                    elif info.get("state") in ("all_pools_old", "no_pool"):
                        if (not cands[k].get("manual")
                                and now - int(cands[k].get("found", now)) > 600):
                            cands.pop(k, None)

            save_state(state)
            if time.time() - last_hb > cfg["heartbeat_seconds"]:
                last_hb = time.time()
                try:
                    sync_state_up(cfg, state, heartbeat=True)
                except Exception as e:
                    log.warning("心跳同步失败: %s", e)

        except Exception as e:
            log.exception("主循环异常: %s", e)
        await asyncio.sleep(max(2, cfg["poll_seconds"] - (time.time() - t0)))


# ---------------------------------------------------------------- 单次/回放（自测用）
async def run_once(token=None, minutes_replay=None):
    """token: 直接评估某合约；minutes_replay: 指定分钟数（回放验证用）"""
    log = setup_logging()
    cfg = load_config()
    if token:
        hit, info = evaluate_token(token, cfg)
        print(json.dumps({"hit": hit, "state": info.get("state"),
                          "m5": info.get("m5"), "minutes": sorted(info.get("minutes", {}).items())},
                         ensure_ascii=False, indent=1))
        if hit and minutes_replay:
            pass
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
