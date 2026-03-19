"""
本地 HTTP bridge：
- POST /send: 向飞书私聊发送一条交互消息
- GET /poll: 阻塞等待用户回复
- POST /reply: 外部注入回复（可选）
- GET /get_id: 返回默认/最近活跃 open_id
- GET /health: 健康检查
"""

import asyncio
import json
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

from feishu_client import FeishuClient


@dataclass
class PendingRequest:
    request_id: str
    created_at: float
    target_open_id: str
    event_type: str
    title: str
    body: str
    options: list[str]
    color: str = "blue"
    message_kind: str = "card"
    message_id: str = ""
    status: str = "pending"
    reply: Optional[str] = None
    replied_at: Optional[float] = None


class BridgeState:
    def __init__(self, default_open_id: str = ""):
        self.default_open_id = default_open_id.strip()
        self.last_active_open_id = ""
        self.last_request_id = ""
        self.last_message_id = ""
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._requests_by_id: dict[str, PendingRequest] = {}
        self._request_id_by_message_id: dict[str, str] = {}

    def set_last_active_open_id(self, open_id: str):
        if not open_id:
            return
        with self._lock:
            self.last_active_open_id = open_id.strip()

    def get_known_open_id(self) -> str:
        with self._lock:
            return self.default_open_id or self.last_active_open_id

    def _target_open_id(self, explicit_open_id: str = "") -> str:
        target = explicit_open_id.strip() or self.default_open_id or self.last_active_open_id
        return target.strip()

    def create_request(
        self,
        event_type: str,
        title: str,
        body: str,
        options: list[str],
        open_id: str = "",
        color: str = "blue",
        message_kind: str = "card",
    ) -> PendingRequest:
        with self._lock:
            target_open_id = self._target_open_id(open_id)
            if not target_open_id:
                raise ValueError("missing target open_id")

            req = PendingRequest(
                request_id=str(uuid.uuid4()),
                created_at=time.time(),
                target_open_id=target_open_id,
                event_type=(event_type or "event").strip(),
                title=(title or "").strip(),
                body=(body or "").strip(),
                options=[str(option).strip() for option in options if str(option).strip()],
                color=(color or "blue").strip(),
                message_kind=(message_kind or "card").strip(),
            )
            self._requests_by_id[req.request_id] = req
            self.last_request_id = req.request_id
            self._cond.notify_all()
            return req

    def bind_message_id(self, request_id: str, message_id: str):
        if not request_id or not message_id:
            return
        with self._lock:
            req = self._requests_by_id.get(request_id)
            if not req:
                return
            req.message_id = message_id
            self._request_id_by_message_id[message_id] = request_id
            self.last_message_id = message_id
            self._cond.notify_all()

    def _get_request_locked(self, request_id: str = "", message_id: str = "") -> Optional[PendingRequest]:
        if request_id:
            return self._requests_by_id.get(request_id)
        if message_id:
            mapped = self._request_id_by_message_id.get(message_id)
            if mapped:
                return self._requests_by_id.get(mapped)
        if self.last_request_id:
            return self._requests_by_id.get(self.last_request_id)
        return None

    def record_reply(self, open_id: str, text: str) -> Optional[PendingRequest]:
        text = text.strip()
        if not open_id or not text:
            return None

        with self._lock:
            candidates = [
                req for req in self._requests_by_id.values()
                if req.status == "pending" and req.target_open_id == open_id
            ]
            if not candidates:
                return None

            req = max(candidates, key=lambda item: item.created_at)
            req.status = "replied"
            req.reply = text
            req.replied_at = time.time()
            self._cond.notify_all()
            return req

    def inject_reply(self, request_id: str = "", message_id: str = "", text: str = "") -> Optional[PendingRequest]:
        text = text.strip()
        if not text:
            return None

        with self._lock:
            req = self._get_request_locked(request_id=request_id.strip(), message_id=message_id.strip())
            if not req:
                return None
            req.status = "replied"
            req.reply = text
            req.replied_at = time.time()
            self._cond.notify_all()
            return req

    def wait_for_reply(
        self,
        timeout: float,
        request_id: str = "",
        message_id: str = "",
    ) -> dict:
        deadline = time.time() + max(timeout, 0)
        request_id = request_id.strip()
        message_id = message_id.strip()

        with self._lock:
            while True:
                req = self._get_request_locked(request_id=request_id, message_id=message_id)
                if not req:
                    return {"error": "unknown_request"}

                if req.status == "replied":
                    return {
                        "request_id": req.request_id,
                        "message_id": req.message_id,
                        "reply": req.reply or "",
                        "target_open_id": req.target_open_id,
                        "type": req.event_type,
                    }

                remaining = deadline - time.time()
                if remaining <= 0:
                    return {
                        "request_id": req.request_id,
                        "message_id": req.message_id,
                        "timeout": True,
                    }
                self._cond.wait(timeout=remaining)


def _render_card_markdown(req: PendingRequest) -> str:
    lines = []
    title = req.title or req.event_type or "待确认事项"
    lines.append(f"## {title}")
    if req.body:
        lines.append(req.body)
    if req.options:
        lines.append("")
        lines.append("请直接回复以下任一选项：")
        for idx, option in enumerate(req.options, 1):
            lines.append(f"{idx}. {option}")
    lines.append("")
    lines.append("也可以直接回复自定义内容。")
    return "\n".join(lines)


class BridgeServer:
    def __init__(self, host: str, port: int, state: BridgeState, feishu: FeishuClient):
        self.host = host
        self.port = port
        self.state = state
        self.feishu = feishu
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        handler = self._build_handler()
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _build_handler(self):
        state = self.state
        feishu = self.feishu
        port = self.port

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args):
                return

            def _json(self, status: int, data: dict):
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _read_json(self) -> tuple[Optional[dict], Optional[str]]:
                content_len = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_len) if content_len else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    return None, "invalid_json"
                if not isinstance(payload, dict):
                    return None, "invalid_json"
                return payload, None

            def do_GET(self):
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)

                if parsed.path == "/health":
                    self._json(
                        200,
                        {
                            "ok": True,
                            "port": port,
                            "default_open_id": bool(state.default_open_id),
                            "last_active_open_id": state.last_active_open_id,
                        },
                    )
                    return

                if parsed.path == "/get_id":
                    open_id = state.get_known_open_id()
                    if not open_id:
                        self._json(404, {"error": "open_id_not_available"})
                        return
                    self._json(200, {"open_id": open_id})
                    return

                if parsed.path != "/poll":
                    self._json(404, {"error": "not_found"})
                    return

                try:
                    timeout = float(params.get("timeout", ["300"])[0])
                except ValueError:
                    timeout = 300.0

                request_id = params.get("request_id", [""])[0]
                message_id = params.get("message_id", [""])[0]
                result = state.wait_for_reply(timeout=timeout, request_id=request_id, message_id=message_id)
                http_status = 404 if result.get("error") == "unknown_request" else 200
                self._json(http_status, result)

            def do_POST(self):
                parsed = urlparse(self.path)
                payload, err = self._read_json()
                if err:
                    self._json(400, {"error": err})
                    return
                assert payload is not None

                if parsed.path == "/send":
                    open_id = str(payload.get("open_id", payload.get("user_id", ""))).strip()
                    event_type = str(payload.get("type", "card")).strip()
                    title = str(payload.get("title", "ARIS Notification"))
                    body = str(payload.get("body", payload.get("content", "")))
                    options = payload.get("options", []) or []
                    color = str(payload.get("color", "blue"))
                    message_kind = "text" if event_type == "text" else str(payload.get("message_type", "card"))
                    if message_kind != "text":
                        message_kind = "card"

                    try:
                        req = state.create_request(
                            event_type=event_type,
                            title=title,
                            body=body,
                            options=options,
                            open_id=open_id,
                            color=color,
                            message_kind=message_kind,
                        )
                    except ValueError as e:
                        self._json(400, {"error": str(e)})
                        return

                    try:
                        if req.message_kind == "text":
                            message_id = asyncio.run(feishu.send_text_to_user(req.target_open_id, req.body or req.title))
                        else:
                            message_id = asyncio.run(
                                feishu.send_card_to_user(
                                    req.target_open_id,
                                    content=_render_card_markdown(req),
                                    loading=False,
                                )
                            )
                    except Exception as e:
                        self._json(502, {"error": f"feishu_send_failed: {e}"})
                        return

                    state.bind_message_id(req.request_id, message_id)
                    self._json(
                        200,
                        {
                            "ok": True,
                            "request_id": req.request_id,
                            "message_id": message_id,
                            "target_open_id": req.target_open_id,
                        },
                    )
                    return

                if parsed.path == "/reply":
                    req = state.inject_reply(
                        request_id=str(payload.get("request_id", "")),
                        message_id=str(payload.get("message_id", "")),
                        text=str(payload.get("text", payload.get("reply", ""))),
                    )
                    if not req:
                        self._json(404, {"error": "unknown_request"})
                        return
                    self._json(
                        200,
                        {
                            "ok": True,
                            "request_id": req.request_id,
                            "message_id": req.message_id,
                        },
                    )
                    return

                self._json(404, {"error": "not_found"})

        return Handler
