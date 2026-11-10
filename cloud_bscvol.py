# -*- coding: utf-8 -*-
"""云端接管（GitHub Actions）：本机心跳缺失时继续跑，回看最近窗口补推漏掉的命中
环境变量：TG_API_ID / TG_API_HASH / TG_STRING_SESSION / GMGN_GH_TOKEN(或 GITHUB_TOKEN)
"""
import asyncio
import os
import sys
import time
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bscvol

CFG_OVERRIDES = {
    "identity": "cloud",
    "max_block_gap": 6000,      # ≈45 分钟
    # 回看分钟线深度。与本机 config.json 保持一致（30 根），
    # 必须 > 发现延迟 + 判定窗口，否则爆发窗口会滑出回看范围造成漏推。
    "deep_minutes": 30,
    "tg_proxy": None,
    "takeover_after": 600,
}
STALE_SECONDS = 900            # 命中窗口结束超过 15 分钟就不补推（已无时效）
DRY = os.environ.get("BSCVOL_DRY", "") == "1"
SELFTEST = os.environ.get("VOL_SELFTEST", "") == "1"
EVAL_BUDGET = 8                # 单次工作流最多评估多少个候选（受 5 分钟节拍与 10 分钟超时约束）
EVAL_TIME_BUDGET = 240          # 单次工作流最多评估多少秒（实测 420s 会跑到 7.5 分钟，撑破节拍）
# GitHub Actions 走共享出口 IP，Gecko 免费配额被全世界共用，429 概率远高于本机。
# 绕过预筛的新币最吃配额（每个至少 2 次 Gecko 请求），因此单独限量。
FRESH_EVAL_LIMIT = 3           # 绕过预筛的新币最多评估几个


def hb_writeback(hb_keep, hb_current):
    """云端收尾写状态时该用哪个心跳：取「本机启动时抓到的」与「当前远端值」中较新的。

    云端接管期间本机可能重启并写下新心跳；若直接写回启动时抓到的旧值，就会把
    本机的新心跳盖掉，外部触发器（按 hb 判断本机是否在线）会继续每 2 分钟 dispatch，
    形成「云端反复接管 + 本机心跳被反复写旧」的自激循环。
    与内核 newer_hb 共用一套「只增不减」语义，避免两处规则漂移。
    """
    return bscvol.newer_hb(hb_keep, hb_current)


def writeback_state(cfg, token, state, hb_keep, log=None):
    """云端收尾：心跳只增不减，整文件写回前先与远端合并 pushed。

    只修 hb 是不够的 —— 云端/本机都把自己内存里的 pushed 全量覆盖回远端，
    会抹掉对端在运行期间新增的去重记录，同一命中就会被推两次。
    """
    remote_end = None
    hb_final = hb_keep
    try:
        remote_end, _ = bscvol.gh_get(cfg, token, cfg["github"]["state_path"])
        hb_final = hb_writeback(hb_keep, (remote_end or {}).get("hb"))
    except Exception as e:
        if log:
            log.warning("收尾读远端心跳失败（沿用启动值）: %s", e)
    # 本机已恢复心跳时不要把身份写成 cloud，避免短暂误导运维判断
    identity = "cloud"
    if local_resumed(hb_final, time.time(), cfg.get("takeover_after", 600)):
        identity = (remote_end or {}).get("identity") or "local"
    bscvol.gh_put_merged(
        cfg, token, cfg["github"]["state_path"],
        {"hb": hb_final, "identity": identity,
         "pushed": state.get("pushed", {}), "target_id": state.get("target_id")},
        "bscvol cloud state [skip ci]")


def local_resumed(hb_iso, now_ts, takeover_after):
    """接管途中本机恢复心跳就返回 True，云端据此让位，避免同一命中被推两次"""
    if not hb_iso:
        return False
    try:
        return (now_ts - bscvol.parse_iso(hb_iso)) < takeover_after
    except Exception:
        return False


async def tg_cloud_client(cfg):
    from telethon import TelegramClient, functions
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(os.environ["VOL_TG_STRING_SESSION"]),
                            int(os.environ["VOL_TG_API_ID"]), os.environ["VOL_TG_API_HASH"],
                            connection_retries=5, retry_delay=2,
                            auto_reconnect=True, timeout=30)
    await client.start()
    return client, functions


def block_rate(chain, cur, blocks=1000):
    """估算 BSC 出块间隔（秒/块），用于把区块高度换算成上线时间"""
    try:
        ts_now = chain.block_ts(cur)
        ts_old = chain.block_ts(cur - blocks)
        rate = (ts_now - ts_old) / float(blocks)
        if 0.05 < rate < 5:
            return rate
    except Exception:
        pass
    return 0.45


async def main():
    log = bscvol.setup_logging("bscvol_cloud")
    cfg = bscvol.load_config()
    cfg.update(CFG_OVERRIDES)
    state = bscvol.load_state()
    token = (os.environ.get(cfg["github"]["token_env"], "")
             or os.environ.get("GITHUB_TOKEN", ""))

    hb_keep = None
    if token:
        remote, _ = bscvol.gh_get(cfg, token, cfg["github"]["state_path"])
        if remote:
            hb_keep = remote.get("hb")
            state.setdefault("pushed", {}).update(remote.get("pushed") or {})
    # 目标群一律以 config.json 的 telegram.chat_id 为准，不接受远端旧值
    if cfg["telegram"].get("chat_id"):
        state["target_id"] = cfg["telegram"]["chat_id"]

    age = None
    if hb_keep:
        try:
            age = time.time() - bscvol.parse_iso(hb_keep)
        except Exception:
            age = None
    log.info("本机心跳 %s（%s 秒前）", hb_keep, None if age is None else int(age))

    if SELFTEST:
        bot = (cfg.get("bot_token") or os.environ.get("VOL_BOT_TOKEN") or "").strip()
        client = functions = None
        if not bot:
            client, functions = await tg_cloud_client(cfg)
            me = await client.get_me()
            who = getattr(me, "username", None) or me.id
        else:
            who = "bot"
        await bscvol.tg_send(client, functions, cfg, state,
                             "【云端链路自检】登录成功：%s，目标群 %s"
                             % (who, state.get("target_id")))
        log.info("自检消息已发送")
        print("SELFTEST OK")
        if client is not None:
            await client.disconnect()
        return

    if age is not None and age < cfg["takeover_after"]:
        log.info("本机在线，本轮不接管")
        print("SKIP: local heartbeat fresh (%ds)" % int(age))
        return

    chain = bscvol.Chain(cfg)
    cur = chain.block_number()
    from_block = cur - cfg["max_block_gap"]
    pairs = chain.paircreated(from_block, cur)
    log.info("扫块 %d-%d，新池 %d 个", from_block, cur, len(pairs))

    rate = block_rate(chain, cur)
    now = int(time.time())
    cands = {}
    for p in pairs:
        # 用区块高度差估算上线时刻（BSC 出块间隔实测约 0.45 秒）
        found = now - int((cur - p.get("block", cur)) * rate)
        for tok in bscvol.pick_token_candidates(p):
            c = cands.setdefault(tok.lower(), {"pairs": [], "found": found})
            c["pairs"].append(p["pair"])
            c["found"] = min(c["found"], found)

    todo = [k for k in cands if not bscvol.is_pushed(state, k)]
    prefilter = cfg["prefilter_m5_usd"]
    bypass = int(cfg.get("fresh_bypass_minutes", 10)) * 60
    fresh = [k for k in todo if now - cands[k]["found"] <= bypass]
    rest = [k for k in todo if k not in fresh]
    m5 = bscvol.dex_prefilter(rest) if rest else {}
    hot = [k for k in rest if m5.get(k, 0) >= prefilter]
    # 上线 10 分钟内的新币绕过预筛直接评估，但共享 IP 下它们最吃配额，限量优先用预算
    fresh = fresh[:FRESH_EVAL_LIMIT]
    queue = hot + fresh
    # 把预筛得到的 m5 作为「历史峰值」传给 evaluate_token：
    # 若某币此刻量已回落但刚爆发过，仍要照常取分钟线，否则会漏推。
    for k in queue:
        if m5.get(k):
            cands[k]["m5_peak"] = float(m5[k])
    log.info("候选 %d（热 %d，新币 %d→限 %d），预筛后 %d",
             len(todo), len(hot), len([k for k in todo if now - cands[k]["found"] <= bypass]),
             FRESH_EVAL_LIMIT, len(queue))

    client = functions = None
    sent = 0
    stale_marked = 0
    rate_limited = False
    t_start = time.time()
    for k in queue:
        if sent >= EVAL_BUDGET or (time.time() - t_start) > EVAL_TIME_BUDGET:
            log.info("评估预算用尽（已推 %d，耗时 %ds），剩余候选留到下一轮", sent, int(time.time() - t_start))
            break
        try:
            hit, info = bscvol.evaluate_token(k, cfg, now, deep_minutes=cfg["deep_minutes"],
                                               m5_peak=cands[k].get("m5_peak"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # 共享 IP 撞到 Gecko 限流：立即停手，不再消耗配额，
                # 剩下的候选留给下一轮（本机在线时云端本就不该接管）
                rate_limited = True
                log.warning("Gecko 限流(429)，本轮停止评估 %s（剩余候选留到下一轮）", k)
                break
            log.warning("评估失败 %s: %s", k, e)
            continue
        except Exception as e:
            log.warning("评估失败 %s: %s", k, e)
            continue
        if not hit:
            continue
        # 推送前复核共享去重库：本机或其它轮次可能在我们评估期间已经推过同一个命中
        if not bscvol.should_push(cfg, state, k, log):
            log.info("本机/其它云端轮次已推送 %s，跳过", k)
            continue
        if now - hit["minute_end"] > STALE_SECONDS:
            # 过期窗口：不推送，但记入 stale —— 不能占用 pushed，
            # 否则该币后续真正成立的新窗口会被永久去重屏蔽
            log.info("命中已过期，跳过 %s（窗口 %s）", k,
                     time.strftime("%H:%M", time.localtime(hit["minute_start"])))
            state.setdefault("stale", {})[k] = {
                "minute_start": hit["minute_start"],
                "at": bscvol.now_iso(),
            }
            stale_marked += 1
            continue
        # 推送前复核一次：挡住 Gecko 在池子初生阶段的异常成交量（实测 2 亿 USD/分钟）
        try:
            hit2, _ = bscvol.evaluate_token(k, cfg, now, deep_minutes=cfg["deep_minutes"],
                                            ignore_age=True, m5_peak=cands[k].get("m5_peak"))
        except Exception as e:
            log.warning("命中复核失败 %s: %s（本轮不推）", k, e)
            hit2 = None
        if not bscvol.push_guard(hit, hit2):
            log.info("命中复核不一致，跳过 %s vols=%s 复核=%s", k,
                     hit.get("vols"), (hit2 or {}).get("vols"))
            continue
        text = bscvol.build_message(k, hit, info, cfg)
        # 接管途中本机若已恢复，云端让位：否则同一个命中会被本机与云端各推一次
        if token:
            try:
                remote_now, _ = bscvol.gh_get(cfg, token, cfg["github"]["state_path"])
                if local_resumed((remote_now or {}).get("hb"), time.time(), cfg["takeover_after"]):
                    log.info("本机已恢复心跳，云端让位，本轮不再推送")
                    break
            except Exception as e:
                log.warning("让位检查失败（继续推送）: %s", e)
        if DRY:
            log.info("[DRY] 会推送 %s %s", k, hit["vols"])
            print(text)
            continue
        try:
            bot = (cfg.get("bot_token") or os.environ.get("VOL_BOT_TOKEN") or "").strip()
            if client is None and not bot:
                client, functions = await tg_cloud_client(cfg)
            await bscvol.tg_send(client, functions, cfg, state, text)
        except Exception as e:
            # 推送失败不能标记已推送，下一轮还会补推
            log.warning("推送失败 %s: %s（不标记已推送，下轮重试）", k, e)
            continue
        bscvol.mark_pushed(state, k, hit)
        sent += 1
        log.info("推送 %s 窗口 %s vols=%s", k,
                 time.strftime("%H:%M", time.localtime(hit["minute_start"])), hit["vols"])

    if client is not None:
        await client.disconnect()

    # DRY 是调试模式，绝不写回共享状态，否则会污染去重库
    if DRY:
        print("DONE[DRY]: pushed=0 candidates=%d hot=%d stale=%d" % (len(cands), len(hot), stale_marked))
        return

    bscvol.save_state(state)
    if token:
        try:
            writeback_state(cfg, token, state, hb_keep, log)
        except Exception as e:
            log.warning("写回云端状态失败: %s", e)
    print("DONE: pushed=%d candidates=%d hot=%d stale=%d rate_limited=%s"
          % (sent, len(cands), len(hot), stale_marked, rate_limited))


if __name__ == "__main__":
    asyncio.run(main())
