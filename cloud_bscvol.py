# -*- coding: utf-8 -*-
"""云端接管（GitHub Actions）：本机心跳缺失时继续跑，回看最近窗口补推漏掉的命中
环境变量：TG_API_ID / TG_API_HASH / TG_STRING_SESSION / GMGN_GH_TOKEN(或 GITHUB_TOKEN)
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bscvol

CFG_OVERRIDES = {
    "identity": "cloud",
    "max_block_gap": 6000,      # ≈45 分钟
    "deep_minutes": 60,         # 回看 60 分钟分钟线
    "tg_proxy": None,
    "takeover_after": 600,
}
STALE_SECONDS = 900            # 命中窗口结束超过 15 分钟就不补推（已无时效）
DRY = os.environ.get("BSCVOL_DRY", "") == "1"
SELFTEST = os.environ.get("VOL_SELFTEST", "") == "1"


async def tg_cloud_client(cfg):
    from telethon import TelegramClient, functions
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(os.environ["VOL_TG_STRING_SESSION"]),
                            int(os.environ["VOL_TG_API_ID"]), os.environ["VOL_TG_API_HASH"],
                            connection_retries=5, retry_delay=2,
                            auto_reconnect=True, timeout=30)
    await client.start()
    return client, functions


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
            if remote.get("target_id"):
                state["target_id"] = remote["target_id"]

    age = None
    if hb_keep:
        try:
            age = time.time() - bscvol.parse_iso(hb_keep)
        except Exception:
            age = None
    log.info("本机心跳 %s（%s 秒前）", hb_keep, None if age is None else int(age))

    if SELFTEST:
        client, functions = await tg_cloud_client(cfg)
        me = await client.get_me()
        entity = await bscvol.tg_ensure_target(client, functions, cfg, state)
        await client.send_message(entity,
                                  "【云端链路自检】登录成功：%s，目标群 %s"
                                  % (getattr(me, "username", None) or me.id,
                                     getattr(entity, "title", entity)),
                                  parse_mode="html", link_preview=False)
        log.info("自检消息已发送")
        print("SELFTEST OK")
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
    cands = {}
    for p in pairs:
        for tok in bscvol.pick_token_candidates(p):
            cands.setdefault(tok.lower(), []).append(p["pair"])

    todo = [k for k in cands if not bscvol.is_pushed(state, k)]
    m5 = bscvol.dex_m5(todo) if todo else {}
    hot = [k for k in todo if m5.get(k, 0) >= cfg["prefilter_m5_usd"]]
    log.info("候选 %d，预筛后 %d", len(todo), len(hot))

    client = functions = None
    sent = 0
    now = int(time.time())
    for k in hot:
        try:
            hit, info = bscvol.evaluate_token(k, cfg, now, deep_minutes=cfg["deep_minutes"])
        except Exception as e:
            log.warning("评估失败 %s: %s", k, e)
            continue
        if not hit:
            continue
        if now - hit["minute_end"] > STALE_SECONDS:
            log.info("命中已过期，跳过 %s（窗口 %s）", k,
                     time.strftime("%H:%M", time.localtime(hit["minute_start"])))
            bscvol.mark_pushed(state, k, hit, reason="stale")
            continue
        text = bscvol.build_message(k, hit, info, cfg)
        if DRY:
            log.info("[DRY] 会推送 %s %s", k, hit["vols"])
            print(text)
        else:
            if client is None:
                client, functions = await tg_cloud_client(cfg)
            entity = await bscvol.tg_ensure_target(client, functions, cfg, state)
            await client.send_message(entity, text, parse_mode="html", link_preview=False)
            sent += 1
        bscvol.mark_pushed(state, k, hit)
        log.info("推送 %s 窗口 %s vols=%s", k,
                 time.strftime("%H:%M", time.localtime(hit["minute_start"])), hit["vols"])

    bscvol.save_state(state)
    if token:
        try:
            bscvol.gh_put(cfg, token, cfg["github"]["state_path"],
                          {"hb": hb_keep, "identity": "cloud",
                           "pushed": state["pushed"], "target_id": state.get("target_id")},
                          "bscvol cloud state [skip ci]")
        except Exception as e:
            log.warning("写回云端状态失败: %s", e)
    if client is not None:
        await client.disconnect()
    print("DONE: pushed=%d candidates=%d hot=%d" % (sent, len(cands), len(hot)))


if __name__ == "__main__":
    asyncio.run(main())
