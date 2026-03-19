"""
飞书 × Claude Code Bot
通过飞书 WebSocket 长连接接收私聊消息，调用本机 claude CLI 回复，支持流式卡片输出。

启动：python main.py
"""

import asyncio
import json
import os
import sys
import threading
import time
import traceback

# 确保项目目录在 sys.path 最前面
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lark_oapi as lark
from lark_oapi.api.im.v1.model import P2ImMessageReceiveV1

import bot_config as config
from claude_runner import run_claude
from commands import handle_command, parse_command
from feishu_client import FeishuClient
from interactive_bridge import BridgeServer, BridgeState
from run_control import ActiveRun, ActiveRunRegistry, stop_run
from session_store import SessionStore

# ── 看门狗：定时重启防止 WebSocket 假死 ──────────────────────

MAX_UPTIME = 4 * 3600
_start_time = time.time()
_last_event = time.time()


def _watchdog():
    """后台线程，定期检查进程健康。异常时退出让 launchctl 拉起。"""
    while True:
        time.sleep(300)
        uptime = time.time() - _start_time
        idle = time.time() - _last_event

        if uptime > MAX_UPTIME:
            print(f"[watchdog] 运行 {uptime/3600:.1f}h，定时重启刷新连接", flush=True)
            os._exit(0)

        print(f"[watchdog] uptime={uptime/3600:.1f}h idle={idle/60:.0f}min", flush=True)


# ── 全局单例 ──────────────────────────────────────────────────

lark_client = lark.Client.builder() \
    .app_id(config.FEISHU_APP_ID) \
    .app_secret(config.FEISHU_APP_SECRET) \
    .log_level(lark.LogLevel.INFO) \
    .build()

feishu = FeishuClient(lark_client, app_id=config.FEISHU_APP_ID, app_secret=config.FEISHU_APP_SECRET)
store = SessionStore()
bridge_state = BridgeState(default_open_id=config.BRIDGE_DEFAULT_OPEN_ID)

_user_locks: dict[str, asyncio.Lock] = {}
_active_runs = ActiveRunRegistry()


def _extract_text_content(msg) -> str:
    if msg.message_type != "text":
        return ""
    try:
        return json.loads(msg.content).get("text", "").strip()
    except Exception:
        return ""


async def _announce_stopped_run(active_run: ActiveRun):
    try:
        await feishu.update_card(active_run.card_msg_id, "⏹ 已停止当前任务")
    except Exception as exc:
        print(f"[warn] update stopped card failed: {exc}", flush=True)


async def _handle_stop_command(sender_open_id: str) -> str:
    active_run = _active_runs.get_run(sender_open_id)
    if active_run is None:
        return "当前没有正在运行的任务"
    if active_run.stop_requested:
        return "正在停止当前任务，请稍候"

    stopped = await stop_run(
        _active_runs,
        sender_open_id,
        on_stopped=_announce_stopped_run,
    )
    if not stopped:
        return "当前没有正在运行的任务"
    return "已发送停止请求"


# ── 核心消息处理（async）─────────────────────────────────────

async def handle_message_async(event: P2ImMessageReceiveV1):
    """异步处理一条飞书消息"""
    msg = event.event.message
    print(f"[收到消息] type={msg.message_type} chat={msg.chat_type}", flush=True)

    if msg.chat_type != "p2p":
        return

    sender_open_id = event.event.sender.sender_id.open_id
    bridge_state.set_last_active_open_id(sender_open_id)

    parsed = parse_command(_extract_text_content(msg))
    if parsed and parsed[0] == "stop":
        reply = await _handle_stop_command(sender_open_id)
        await feishu.send_card_to_user(sender_open_id, content=reply, loading=False)
        return

    if sender_open_id not in _user_locks:
        _user_locks[sender_open_id] = asyncio.Lock()
    lock = _user_locks[sender_open_id]

    async with lock:
        try:
            await _process_message(sender_open_id, msg)
        except Exception as e:
            print(f"[error] 消息处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()


async def _process_message(sender_open_id: str, msg):
    """实际处理消息的逻辑，在 per-user lock 保护下执行"""
    text = ""

    if msg.message_type == "text":
        text = _extract_text_content(msg)
        if not text:
            return

        pending = bridge_state.record_reply(sender_open_id, text)
        if pending:
            print(f"[bridge] 收到回复 request={pending.request_id[:8]} reply={text[:50]}", flush=True)
            await feishu.send_text_to_user(sender_open_id, f"已收到回复：{text}")
            return

        print(f"[处理消息] sender={sender_open_id[:8]}...", flush=True)
        print(f"[文本] {text[:50]}", flush=True)

    elif msg.message_type == "image":
        print(f"[处理消息] sender={sender_open_id[:8]}...", flush=True)
        try:
            image_key = json.loads(msg.content).get("image_key", "")
            if not image_key:
                return
            img_path = await feishu.download_image(msg.message_id, image_key)
            text = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片，直接回复用中文]"
        except Exception as e:
            print(f"[error] 下载图片失败: {e}", flush=True)
            await feishu.send_text_to_user(sender_open_id, f"❌ 下载图片失败：{e}")
            return

    else:
        return

    parsed = parse_command(text)
    if parsed:
        cmd, args = parsed
        reply = handle_command(cmd, args, sender_open_id, store)
        if reply is not None:
            if cmd == "resume" and not args:
                await feishu.send_text_to_user(sender_open_id, reply)
            else:
                await feishu.send_card_to_user(sender_open_id, content=reply, loading=False)
            return

    session = store.get_current(sender_open_id)
    print(f"[Claude] session={session.session_id} model={session.model}", flush=True)

    try:
        card_msg_id = await feishu.send_card_to_user(sender_open_id, loading=True)
        print(f"[卡片] card_msg_id={card_msg_id}", flush=True)
    except Exception as e:
        print(f"[error] 发送占位卡片失败: {e}", flush=True)
        await feishu.send_text_to_user(sender_open_id, f"❌ 发送消息失败：{e}")
        return

    active_run = _active_runs.start_run(sender_open_id, card_msg_id)
    accumulated = ""
    chars_since_push = 0

    async def push(content: str):
        try:
            await feishu.update_card(card_msg_id, content)
        except Exception as push_err:
            print(f"[warn] push 失败: {push_err}", flush=True)

    async def on_tool_use(name: str, inp: dict):
        nonlocal accumulated, chars_since_push
        if name.lower() == "askuserquestion":
            question = inp.get("question", inp.get("text", ""))
            if question:
                accumulated += f"\n\n❓ **等待回复：**\n{question}"
                chars_since_push = 0
                await push(accumulated)
                return
        tool_line = _format_tool(name, inp)
        display = f"{tool_line}\n\n{accumulated}" if accumulated else tool_line
        await push(display)

    async def on_text_chunk(chunk: str):
        nonlocal accumulated, chars_since_push
        accumulated += chunk
        chars_since_push += len(chunk)
        if chars_since_push >= config.STREAM_CHUNK_SIZE:
            await push(accumulated)
            chars_since_push = 0

    claude_msg = text
    if not session.session_id:
        claude_msg = (
            "[环境：用户通过飞书发送消息，无交互式UI。"
            "当需要用户做选择时，用编号列表呈现选项（1. 2. 3.），"
            "最后加一个「其他（请说明）」选项，用户回复数字即可。"
            "简单确认用 Y/N。]\n\n" + text
        )

    try:
        print("[run_claude] 开始调用...", flush=True)
        full_text, new_session_id = await run_claude(
            message=claude_msg,
            session_id=session.session_id,
            model=session.model,
            cwd=session.cwd,
            permission_mode=session.permission_mode,
            on_text_chunk=on_text_chunk,
            on_tool_use=on_tool_use,
            on_process_start=lambda proc: _active_runs.attach_process(sender_open_id, proc),
        )
        print(f"[run_claude] 完成, session={new_session_id}", flush=True)
    except Exception as e:
        if active_run.stop_requested:
            return
        print(f"[error] Claude 运行失败: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        await push(f"❌ Claude 执行出错：{type(e).__name__}: {e}")
        return
    finally:
        _active_runs.clear_run(sender_open_id, active_run)

    if active_run.stop_requested:
        return

    final = full_text or accumulated or "（无输出）"
    await push(final)

    if new_session_id:
        store.on_claude_response(sender_open_id, new_session_id, text)


def _format_tool(name: str, inp: dict) -> str:
    """格式化工具调用的进度提示"""
    n = name.lower()
    if n == "bash":
        cmd = inp.get("command", "")
        if len(cmd) > 80:
            cmd = cmd[:77] + "..."
        return f"🔧 **执行命令：** `{cmd}`" if cmd else "🔧 **执行命令...**"
    if n in ("read_file", "read"):
        return f"📄 **读取：** `{inp.get('file_path', inp.get('path', ''))}`"
    if n in ("write_file", "write"):
        return f"✏️ **写入：** `{inp.get('file_path', inp.get('path', ''))}`"
    if n in ("edit_file", "edit"):
        return f"✂️ **编辑：** `{inp.get('file_path', inp.get('path', ''))}`"
    if n == "glob":
        return f"🔍 **搜索文件：** `{inp.get('pattern', '')}`"
    if n == "grep":
        return f"🔎 **搜索内容：** `{inp.get('pattern', '')}`"
    if n == "task":
        return f"🤖 **子任务：** {inp.get('description', inp.get('prompt', '')[:40])}"
    if n == "webfetch":
        return "🌐 **抓取网页...**"
    if n == "websearch":
        return f"🔍 **搜索：** `{inp.get('query', '')}`"
    return f"⚙️ **{name}**"


# ── 飞书事件回调（同步）→ 调度异步任务 ───────────────────────

def on_message_receive(data: P2ImMessageReceiveV1) -> None:
    global _last_event
    _last_event = time.time()
    asyncio.ensure_future(handle_message_async(data))


# ── 启动 ──────────────────────────────────────────────────────

def main():
    print("🚀 飞书 Claude Bot 启动中...")
    print(f"   App ID      : {config.FEISHU_APP_ID}")
    print(f"   默认模型    : {config.DEFAULT_MODEL}")
    print(f"   默认工作目录: {config.DEFAULT_CWD}")
    print(f"   权限模式    : {config.PERMISSION_MODE}")
    print(f"   Bridge 地址 : http://{config.BRIDGE_HOST}:{config.BRIDGE_PORT}")

    bridge_server = BridgeServer(
        host=config.BRIDGE_HOST,
        port=config.BRIDGE_PORT,
        state=bridge_state,
        feishu=feishu,
    )
    bridge_server.start()

    handler = lark.EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(on_message_receive) \
        .build()

    ws_client = lark.ws.Client(
        config.FEISHU_APP_ID,
        config.FEISHU_APP_SECRET,
        event_handler=handler,
        log_level=lark.LogLevel.INFO,
    )

    t = threading.Thread(target=_watchdog, daemon=True)
    t.start()

    print("✅ 连接飞书 WebSocket 长连接（自动重连）...")
    ws_client.start()


if __name__ == "__main__":
    main()
